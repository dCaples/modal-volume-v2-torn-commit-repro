# Modal Volume v2: `commit()` racing in-flight writes publishes files that read as all zeros

A `volume.commit()` that overlaps in-flight writes **in the same container** can seal a
snapshot containing a file's directory entry and full size but not its data blocks.
Other containers that `reload()` then read that file **successfully** — correct length,
every byte zero. A later commit heals the file, so the corruption is a transient but
*committed* window that any concurrent reader can capture and propagate downstream.

This is a single-writer scenario: there is no multi-container write conflict for
"last write wins" to resolve. One container wrote the file once (via the standard
`tempfile` + `os.replace` atomic pattern) and its own commit published a state that
never existed on any writer: new entry + new size + zero data.

## Repro

```bash
pip install modal   # observed on modal==1.4.2
modal run repro_vol_tear.py
```

Takes about one minute end to end. Prints `REPRO: torn/zero reads observed` or
`NO-REPRO`. Uses a scratch v2 volume `vol-v2-tear-repro-scratch` (created if missing);
delete it afterwards with `modal volume delete vol-v2-tear-repro-scratch`.

What the script does:

- **writer** container: one thread loops `volume.commit()` while another writes bursts
  of 85 × 917,033-byte files (deterministic non-zero content derived from each
  filename), each written with `tempfile.mkstemp` + `os.replace`. First to fresh
  paths, then repeatedly replacing the same paths with identical bytes.
- **reader** container: loops `volume.reload()` + reads the recently written files and
  compares against the expected content. On any mismatch it immediately re-reads the
  file to distinguish a transient read glitch from a committed state.

## Observed results (2026-07-31, modal 1.4.2)

First run, no tuning:

- **131 of 425 reads (~31%) returned all-zero content** at exactly the correct file
  size (917,033 bytes, 917,033 zero bytes).
- Anomalies arrive in contiguous chunks of a burst (e.g. `fresh0006/f036.bin` through
  `f045.bin` all zero) — consistent with a single commit sealing mid-burst.
- **Every immediate re-read was still all zeros** — the reader is not racing a write;
  it is reading the published, committed state.
- After the writer's final commit, the same files read back with the correct bytes
  (verified out-of-band via `modal volume get`): the volume converges, but readers
  during the window got fabricated data with no error.

Example reader output:

```
[reader] ANOMALY {'path': '/vol/objects/fresh0006/f036.bin', 'kind': 'MISMATCH',
 'len': 917033, 'zeros': 917033, 'all_zero': True,
 'reread': 'still-bad(len=917033,zeros=917033)', 't': 7.13}
```

## Why we think this is a bug and not the documented concurrency limitation

- The documented caveat for Volumes ("concurrent modification of the same file from
  multiple containers — last write wins, unsynchronized data may be lost") describes
  conflicting **writers**. This repro has one writer container; the conflict is between
  that container's own writes and its own (or a background) commit. With
  `@modal.concurrent` inputs sharing a container and per-input commits — and with v2's
  background commits, which the user does not control — this overlap is unavoidable in
  normal usage.
- `os.replace` is the standard atomicity primitive: readers should observe the old
  file or the new file. Here readers observe a third state that no process ever wrote.
- The failure mode is the worst available one: the read **succeeds** with a
  plausible-length buffer of zeros. An exclusion of the in-flight file from the
  snapshot (reader sees `FileNotFoundError` and retries), a failed commit, or `EIO`
  on unresolvable blocks would all be recoverable; silently fabricated zeros defeat
  length checks and propagate as valid data.

## How this bit us in production

In a content-addressed object store on a v2 volume (worker containers write
`objects/<sha256>.bin` + a marker; a drainer container uploads marked objects to R2),
the drainer read an object during this window and uploaded 917,033 zero bytes to R2
under a key asserting the correct content hash. The volume copy later healed, but the
object store's cold tier now served hash-mismatched zeros, which deterministically
crashed downstream jobs that trusted the content address. We are adding hash
verification on the drain path and serializing commits against in-flight writes on
our side regardless — but reads that fabricate never-written data are not something
callers can be expected to defend against in general.

## Environment

- `modal==1.4.2`, Volume v2 (`modal.Volume.from_name(..., version=2)`)
- Writer/reader: `debian_slim` image, 4 CPU, default region selection
- Observed 2026-07-31 in the `markovrobotics` workspace
