# DeltaForge ADBC: client tests

Public-facing tests you can run against an **installed DeltaForge ADBC driver**.
They drive the driver through the public ADBC API (`adbc_driver_manager` in
Python, the ADBC .NET binding), exactly as your own application would, and
verify read/write behavior end to end. Each test skips cleanly (exit 77) when it
cannot connect, so they are safe to run in CI without a backend.

These tests exercise the shipped binary. They do not contain or require the
driver's source.

## Prerequisites

- The DeltaForge ADBC driver, either `pip install deltaforge-adbc` (bundles the
  native driver) or the native driver from the Releases page.
- A reachable DeltaForge control plane + compute node, and the target tables
  already created.

## Python

```bash
pip install deltaforge-adbc pyarrow
export DELTAFORGE_CONTROL_PLANE_URL=https://control.example.com
export DELTAFORGE_SESSION_TOKEN=df_pat_...
export DELTAFORGE_INGEST_TARGET=your.schema.table   # must already exist
# If using the native driver instead of the wheel, also set:
#   export DELTAFORGE_ADBC_PATH=/path/to/libdeltaforge_adbc.so
python -m pytest tests/python -v
```

Covers append, overwrite (replace), upsert, idempotent replay, schema coercion,
strict-mode rejection, and concurrent writes.

## .NET

```bash
export DELTAFORGE_CONTROL_PLANE_URL=https://control.example.com
export DELTAFORGE_SESSION_TOKEN=df_pat_...
export DELTAFORGE_INGEST_TARGET=your.schema.table
dotnet run --project tests/dotnet
```

## Exit codes

`0` all passed, `1` one or more failed, `77` skipped (could not connect).
