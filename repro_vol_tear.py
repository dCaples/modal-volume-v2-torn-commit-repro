"""Repro: Modal Volume v2 — commit() racing in-flight writes in the SAME container
publishes files that other containers read as all zeros.

Setup:
  - writer container: one thread loops volume.commit() while another thread writes
    bursts of 85 x 917,033-byte files, each via the standard atomic pattern
    (tempfile.mkstemp + write + os.replace).
  - reader container: loops volume.reload() + reads the files, comparing against
    the expected deterministic content. On mismatch it immediately re-reads to
    distinguish a transient read glitch from a committed state.

Observed (modal 1.4.2, 2026-07-31): ~31% of reads under contention return files at
exactly the correct length containing 100% zero bytes. Immediate re-reads still
return zeros (it is the committed, published state, not a read race). A later
commit heals the file — so the corruption is a transient-but-committed window
that any concurrent reader can capture and propagate.

Run:  modal run repro_vol_tear.py
Takes ~1 minute. Prints REPRO / NO-REPRO at the end.

Uses a scratch volume `vol-v2-tear-repro-scratch` (created if missing).
"""

from __future__ import annotations

import hashlib

import modal

app = modal.App("vol-v2-tear-repro")

VOLUME_NAME = "vol-v2-tear-repro-scratch"
MOUNT = "/vol"
FILE_SIZE = 917_033  # deliberately sub-1MiB and not block-aligned
BURST = 85           # files per write burst
FRESH_ROUNDS = 15
REWRITE_ITERS = 150
BURST_THREADS = 8

vol = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True, version=2)
image = modal.Image.debian_slim()


def pattern_bytes(name: str) -> bytes:
    """Deterministic non-zero content for a file, recomputable by the reader."""
    seed = hashlib.sha256(name.encode()).digest()
    reps = FILE_SIZE // len(seed) + 1
    return (seed * reps)[:FILE_SIZE]


def _write_one(dirpath: str, fname: str, payload: bytes) -> None:
    """Standard atomic-write pattern: temp file in the same dir, then os.replace."""
    import os
    import tempfile

    path = os.path.join(dirpath, fname)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{fname}.", dir=dirpath)
    try:
        with os.fdopen(fd, "wb") as tmp:
            tmp.write(payload)
        os.replace(tmp_name, path)
    except BaseException:
        import contextlib

        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise


@app.function(image=image, volumes={MOUNT: vol}, timeout=3600, cpu=4)
def writer() -> dict:
    import os
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    stop = threading.Event()
    stats = {"commits": 0, "commit_errors": 0}

    def committer() -> None:
        while not stop.is_set():
            try:
                vol.commit()
                stats["commits"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["commit_errors"] += 1
                print(f"[writer] commit error: {exc!r}", flush=True)

    commit_thread = threading.Thread(target=committer, daemon=True)
    commit_thread.start()

    pool = ThreadPoolExecutor(max_workers=BURST_THREADS)
    t0 = time.time()

    def burst(dirname: str) -> None:
        d = os.path.join(MOUNT, "objects", dirname)
        os.makedirs(d, exist_ok=True)
        list(
            pool.map(
                lambda i: _write_one(d, f"f{i:03d}.bin", pattern_bytes(f"{dirname}/{i}")),
                range(BURST),
            )
        )

    # Phase 1: bursts of brand-new files.
    for r in range(FRESH_ROUNDS):
        burst(f"fresh{r:04d}")
        if r % 5 == 0:
            print(f"[writer] fresh round {r} t={time.time()-t0:.0f}s commits={stats['commits']}", flush=True)
        time.sleep(0.1)

    # Phase 2: repeatedly replace the SAME paths with identical bytes.
    for it in range(REWRITE_ITERS):
        burst("rw")
        if it % 25 == 0:
            print(f"[writer] rewrite iter {it} t={time.time()-t0:.0f}s commits={stats['commits']}", flush=True)

    stop.set()
    commit_thread.join(timeout=30)
    with open(os.path.join(MOUNT, "done.flag"), "w") as f:
        f.write("done")
    vol.commit()
    stats["elapsed_s"] = round(time.time() - t0, 1)
    print(f"[writer] DONE {stats}", flush=True)
    return stats


@app.function(image=image, volumes={MOUNT: vol}, timeout=3600, cpu=4)
def reader(duration_s: int) -> dict:
    import os
    import time

    anomalies: list[dict] = []
    stats = {"reads": 0, "reloads": 0, "reload_errors": 0, "read_errors": 0}
    t0 = time.time()

    def check(dirname: str, i: int) -> None:
        path = os.path.join(MOUNT, "objects", dirname, f"f{i:03d}.bin")
        try:
            with open(path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            return
        except OSError as exc:
            stats["read_errors"] += 1
            anomalies.append({"path": path, "kind": "read-error", "detail": repr(exc), "t": round(time.time() - t0, 2)})
            return
        stats["reads"] += 1
        expect = pattern_bytes(f"{dirname}/{i}")
        if data == expect:
            return
        zeros = data.count(0)
        # Immediate re-read: transient glitch, or the committed state?
        try:
            with open(path, "rb") as f:
                data2 = f.read()
            reread = "healed" if data2 == expect else f"still-bad(len={len(data2)},zeros={data2.count(0)})"
        except OSError as exc:
            reread = f"reread-error {exc!r}"
        anomaly = {
            "path": path,
            "kind": "MISMATCH",
            "len": len(data),
            "zeros": zeros,
            "all_zero": zeros == len(data),
            "reread": reread,
            "t": round(time.time() - t0, 2),
        }
        anomalies.append(anomaly)
        print(f"[reader] ANOMALY {anomaly}", flush=True)

    while time.time() - t0 < duration_s:
        try:
            vol.reload()
            stats["reloads"] += 1
        except Exception as exc:  # noqa: BLE001
            stats["reload_errors"] += 1
            if stats["reload_errors"] <= 3:
                print(f"[reader] reload error: {exc!r}", flush=True)
        base = os.path.join(MOUNT, "objects")
        try:
            dirs = sorted(os.listdir(base))
        except FileNotFoundError:
            time.sleep(0.5)
            continue
        # Sweep the rewrite dir every pass plus the two newest fresh dirs.
        targets = [d for d in dirs if d == "rw"] + [d for d in dirs if d.startswith("fresh")][-2:]
        for d in targets:
            for i in range(BURST):
                check(d, i)
        if int(time.time() - t0) % 30 < 1:
            print(f"[reader] t={time.time()-t0:.0f}s {stats} anomalies={len(anomalies)}", flush=True)
        if os.path.exists(os.path.join(MOUNT, "done.flag")):
            print("[reader] writer done flag seen — exiting", flush=True)
            break

    print(f"[reader] DONE {stats} anomalies={len(anomalies)}", flush=True)
    return {"stats": stats, "anomalies": anomalies}


@app.local_entrypoint()
def main() -> None:
    print(f"volume={VOLUME_NAME} file_size={FILE_SIZE} burst={BURST} "
          f"fresh_rounds={FRESH_ROUNDS} rewrite_iters={REWRITE_ITERS}")
    reader_call = reader.spawn(duration_s=1500)
    writer_stats = writer.remote()
    print(f"writer finished: {writer_stats}")
    result = reader_call.get()
    print(f"reader stats: {result['stats']}")
    print(f"anomalies: {len(result['anomalies'])}")
    for a in result["anomalies"][:50]:
        print(f"  {a}")
    if result["anomalies"]:
        print("REPRO: torn/zero reads observed")
    else:
        print("NO-REPRO: all reads matched expected content")
