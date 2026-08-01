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
pip install modal   # reproduced on modal==1.4.2 and 1.5.0
modal run repro_vol_tear.py
```

The race is probabilistic (a run samples a limited number of committed snapshots):
observed 5/6 runs reproducing across modal 1.4.2 and 1.5.0. Delete the scratch
volume (`modal volume delete vol-v2-tear-repro-scratch --yes`) and repeat if the
first run reports NO-REPRO.

`modal run repro_vol_tear.py --no-commit-loop --rounds 200` writes with **no
explicit commits at all**. This mode also reproduces — worse (362 and 218 torn
reads in two control runs): the platform's background commits publish the same
torn state on their own. User commit discipline cannot avoid this bug.
