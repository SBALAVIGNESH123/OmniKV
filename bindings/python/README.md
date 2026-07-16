# OmniKV Python bridge

This package exposes OmniKV's stable embedded Rust API to Python.

It is intentionally small and synchronous so Python applications can use the
same durable embedded key-value contract as Rust callers.

## Install from the repository

```bash
python -m pip install maturin
python -m pip install ./bindings/python
```

## Use directly

```python
import omnikv

db = omnikv.open_embedded("data/omnikv", namespace="sketchlog")
db.put("hello", "world")
assert db.get("hello") == "world"
rows = db.scan_prefix("he")
db.sync()
db.close()
```

## SketchLog contract

SketchLog expects the module to expose:

- `open_embedded(data_dir, namespace="sketchlog")`
- `EmbeddedOmniKv.open(data_dir, namespace="sketchlog")`
- `EmbeddedOmniKv.open_dir(data_dir)` plus optional `.scoped(namespace)`
- client methods: `put`, `get`, `delete`, `scan_prefix`
- optional methods: `sync`, `close`, `stats`
