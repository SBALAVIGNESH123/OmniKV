import tempfile

import omnikv


def test_python_bridge_contract_survives_reopen() -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        db = omnikv.open_embedded(data_dir, namespace="sketchlog")
        sequence = db.put("stream/a", "payload-1")

        assert isinstance(sequence, int)
        assert sequence > 0
        assert db.get("stream/a") == "payload-1"
        assert db.scan_prefix("stream/") == [
            {"key": "stream/a", "value": "payload-1"}
        ]
        assert db.stats()["sequence"] >= sequence

        db.sync()
        db.close()

        reopened = omnikv.EmbeddedOmniKv.open(data_dir, namespace="sketchlog")
        assert reopened.get("stream/a") == "payload-1"

        other_namespace = reopened.scoped("other")
        assert other_namespace.get("stream/a") is None
        other_namespace.close()

        reopened.delete("stream/a")
        reopened.sync()
        assert reopened.get("stream/a") is None
        reopened.close()

        root = omnikv.EmbeddedOmniKv.open_dir(data_dir)
        scoped = root.scoped("sketchlog")
        assert scoped.get("stream/a") is None
        scoped.close()
        root.close()


def test_closed_python_bridge_handle_rejects_operations() -> None:
    with tempfile.TemporaryDirectory() as data_dir:
        db = omnikv.open_embedded(data_dir, namespace="sketchlog")
        db.close()

        try:
            db.get("stream/a")
        except RuntimeError as exc:
            assert "closed" in str(exc)
        else:
            raise AssertionError("closed handle should reject reads")
