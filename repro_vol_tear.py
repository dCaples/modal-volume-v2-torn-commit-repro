"""Repro: Modal Volume v2 — commit() racing in-flight writes in the SAME container
publishes files that other containers read as all zeros.

Setup:
  - writer container: one thread loops volume.commit() while another thread writes
    bursts of 85 x 917,033-byte brand-new files, each via the standard atomic
    pattern (tempfile.mkstemp + write + os.replace).
  - reader containers: loop volume.reload() + read a small rotating sample of the
    newest files, comparing against the expected deterministic content. On mismatch
    they immediately re-read to distinguish a transient glitch from a committed
    state. A reader's view only changes on reload(), so the repro keeps sweeps
    small to sample as many committed snapshots as possible.

Observed (modal 1.4.2 and 1.4.x-era runtimes, 2026-07-31): reads under contention
return files at exactly the correct length containing 100% zero bytes. Immediate
re-reads still return zeros (it is the committed, published state, not a read
race). A later commit heals the file — so the corruption is a transient-but-
committed window that any concurrent reader can capture and propagate. The race
is probabilistic: a run samples a limited number of snapshots, so repeat a few
times if the first run reports NO-REPRO.

Run:  modal run repro_vol_tear.py
Takes ~1.5 minutes. Prints REPRO / NO-REPRO at the end.

Uses a scratch volume `vol-v2-tear-repro-scratch` (created if missing). Delete the
volume between runs so every round writes brand-new paths:
  modal volume delete vol-v2-tear-repro-scratch --yes
"""

from __future__ import annotations

import hashlib

import modal

app = modal.App("vol-v2-tear-repro")

VOLUME_NAME = "vol-v2-tear-repro-scratch"
MOUNT = "/vol"
FILE_SIZE = 917_033  # deliberately sub-1MiB and not block-aligned
BURST = 85           # files per write burst
ROUNDS = 60
BURST_THREADS = 8
READERS = 2
SAMPLE_PER_PASS = 20  # small sweeps -> frequent reload() -> more snapshots sampled

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
def writer(commit_loop: bool = True, rounds: int = ROUNDS) -> dict:
    import os
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    stop = threading.Event()
    stats = {"commits": 0, "commit_errors": 0, "commit_loop": commit_loop}

    def committer() -> None:
        while not stop.is_set():
            try:
                vol.commit()
                stats["commits"] += 1
            except Exception as exc:  # noqa: BLE001
                stats["commit_errors"] += 1
                print(f"[writer] commit error: {exc!r}", flush=True)

    commit_thread = threading.Thread(target=committer, daemon=True)
    if commit_loop:
        commit_thread.start()

    pool = ThreadPoolExecutor(max_workers=BURST_THREADS)
    t0 = time.time()

    for r in range(rounds):
        dirname = f"fresh{r:04d}"
        d = os.path.join(MOUNT, "objects", dirname)
        os.makedirs(d, exist_ok=True)
        list(
            pool.map(
                lambda i: _write_one(d, f"f{i:03d}.bin", pattern_bytes(f"{dirname}/{i}")),
                range(BURST),
            )
        )
        if r % 10 == 0:
            print(f"[writer] round {r} t={time.time()-t0:.0f}s commits={stats['commits']}", flush=True)
        time.sleep(0.15)

    stop.set()
    if commit_loop:
        commit_thread.join(timeout=30)
    with open(os.path.join(MOUNT, "done.flag"), "w") as f:
        f.write("done")
    vol.commit()
    stats["elapsed_s"] = round(time.time() - t0, 1)
    print(f"[writer] DONE {stats}", flush=True)
    return stats


@app.function(image=image, volumes={MOUNT: vol}, timeout=3600, cpu=4)
def reader(reader_id: int, duration_s: int) -> dict:
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
        if len(anomalies) <= 10 or len(anomalies) % 50 == 0:
            print(f"[reader{reader_id}] ANOMALY #{len(anomalies)} {anomaly}", flush=True)

    passes = 0
    while time.time() - t0 < duration_s:
        try:
            vol.reload()
            stats["reloads"] += 1
        except Exception as exc:  # noqa: BLE001
            stats["reload_errors"] += 1
            if stats["reload_errors"] <= 3:
                print(f"[reader{reader_id}] reload error: {exc!r}", flush=True)
        base = os.path.join(MOUNT, "objects")
        try:
            dirs = sorted(d for d in os.listdir(base) if d.startswith("fresh"))
        except FileNotFoundError:
            time.sleep(0.3)
            continue
        # Small rotating sample from the two newest dirs; readers offset from each other.
        for depth, dirname in enumerate(dirs[-2:][::-1]):
            n = SAMPLE_PER_PASS if depth == 0 else SAMPLE_PER_PASS // 2
            start = (passes * SAMPLE_PER_PASS + reader_id * SAMPLE_PER_PASS // READERS) % BURST
            for k in range(n):
                check(dirname, (start + k) % BURST)
        passes += 1
        if passes % 20 == 0:
            print(f"[reader{reader_id}] t={time.time()-t0:.0f}s {stats} anomalies={len(anomalies)}", flush=True)
        if os.path.exists(os.path.join(MOUNT, "done.flag")):
            print(f"[reader{reader_id}] writer done flag seen — exiting", flush=True)
            break

    print(f"[reader{reader_id}] DONE {stats} anomalies={len(anomalies)}", flush=True)
    return {"stats": stats, "anomalies": anomalies}


@app.local_entrypoint()
def main(commit_loop: bool = True, rounds: int = ROUNDS) -> None:
    print(f"volume={VOLUME_NAME} file_size={FILE_SIZE} burst={BURST} rounds={rounds} "
          f"readers={READERS} commit_loop={commit_loop}")
    reader_calls = [reader.spawn(reader_id=i, duration_s=1500) for i in range(READERS)]
    writer_stats = writer.remote(commit_loop=commit_loop, rounds=rounds)
    print(f"writer finished: {writer_stats}")
    all_anomalies = []
    for i, call in enumerate(reader_calls):
        result = call.get()
        print(f"reader{i} stats: {result['stats']} anomalies={len(result['anomalies'])}")
        all_anomalies.extend(result["anomalies"])
    for a in all_anomalies[:50]:
        print(f"  {a}")
    print(f"anomalies: {len(all_anomalies)}")
    if all_anomalies:
        print("REPRO: torn/zero reads observed")
    else:
        print("NO-REPRO: all reads matched expected content")
