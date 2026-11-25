#!/usr/bin/env python3
"""Run every Cargo integration-test target not covered by performance jobs.

The workflow keeps dedicated jobs for the benchmark and storage-performance
targets because those suites intentionally have different release profiles
and runtime characteristics. This script discovers every other integration
target from Cargo metadata, so adding a new test binary cannot silently leave
it outside the CI gate.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PERFORMANCE_TARGETS = {"benchmarks", "storage_perf"}


def main() -> int:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=ROOT,
            text=True,
        )
    )

    targets = sorted(
        (package["name"], target["name"])
        for package in metadata["packages"]
        for target in package["targets"]
        if "test" in target["kind"] and target["name"] not in PERFORMANCE_TARGETS
    )

    if not targets:
        raise SystemExit("cargo metadata reported no integration-test targets")

    for package, target in targets:
        print(f"==> cargo test -p {package} --test {target}", flush=True)
        subprocess.run(
            [
                "cargo",
                "test",
                "-p",
                package,
                "--test",
                target,
                "--locked",
                "--",
                "--test-threads=1",
            ],
            cwd=ROOT,
            check=True,
        )

    print(f"Ran {len(targets)} non-performance integration-test targets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
