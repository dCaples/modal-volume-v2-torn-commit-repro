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
