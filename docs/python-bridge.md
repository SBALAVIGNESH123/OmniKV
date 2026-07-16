# Python embedded bridge

OmniKV ships a Python/native bridge for the stable embedded storage API. The
bridge is intentionally thin: Python callers get a synchronous key-value handle
over the same Rust `EmbeddedOmniKv` facade used by native integrations.

## Install from source

```bash
python -m pip install "maturin>=1.14,<2"
python -m pip install ./bindings/python
```

## Contract

The module name is `omnikv`.

Supported factory shapes:

```python
import omnikv

db = omnikv.open_embedded("data/omnikv", namespace="sketchlog")
db = omnikv.EmbeddedOmniKv.open("data/omnikv", namespace="sketchlog")
db = omnikv.EmbeddedOmniKv.open_dir("data/omnikv").scoped("sketchlog")
```

Supported methods:

- `put(key: str, value: str) -> int`
- `get(key: str) -> str | None`
- `delete(key: str) -> int`
- `scan_prefix(prefix: str, limit: int | None = None) -> list[dict]`
- `sync() -> None`
- `close() -> None`
- `stats() -> dict`

`scan_prefix` returns rows shaped as dictionaries with `key` and `value`
fields. This matches SketchLog's embedded storage adapter contract.

## SketchLog usage

After installing the bridge and SketchLog in the same Python environment:

```bash
export SKETCHLOG_STORAGE_BACKEND=omnikv
export SKETCHLOG_OMNIKV_DATA_DIR=/var/lib/sketchlog/omnikv
export SKETCHLOG_OMNIKV_NAMESPACE=sketchlog
export SKETCHLOG_OMNIKV_MODULE=omnikv
```

Then start SketchLog normally. SketchLog imports `omnikv` and opens an embedded
handle through `open_embedded(data_dir, namespace=...)`.

## Validate locally

```bash
python -m pip install ./bindings/python
python -m pytest bindings/python/tests
```
