# DeltaForge ADBC Driver

Arrow-native connectivity to DeltaForge. The ADBC (Arrow Database Connectivity)
driver exposes query results as Arrow record batches end to end, with no
row-at-a-time marshalling, so it is the fastest path for wide-column scans and
the preferred way to write DataFrames straight into Delta tables. Power BI
Desktop uses ADBC as its native Arrow driver class; this driver also works with
any Apache Arrow ADBC client and from Python.

DeltaForge is commercial software with a free Community license (full platform,
single node). See [deltaforge.org/pricing](https://deltaforge.org/pricing).

## Install

Pick the path that matches how you connect:

- **Python (pip)** for pandas / polars / pyarrow workflows:
  ```bash
  pip install deltaforge-adbc
  # optional DataFrame integrations:
  pip install "deltaforge-adbc[pandas]"   # or [polars]
  ```
  The wheel bundles the native driver for your platform; nothing else to set up.

- **Native driver** (for ADBC clients that load a shared library directly):
  download the installer for your OS from the
  [Releases](https://github.com/deltaforge-org/delta-forge-adbc/releases) page:
  - Linux: `.deb` / `.rpm`, or the `libdeltaforge_adbc.so` tarball
  - macOS: `.pkg`, or the `libdeltaforge_adbc.dylib` tarball
  - Windows: `.msi`, or the `deltaforge_adbc.dll` zip

- **Power BI**: install the native driver, then the `DeltaForgeAdbc.mez` custom
  connector (also on the Releases page). Power BI Desktop 2.145.1105.0+ is
  required for the ADBC driver class; Service refresh needs the on-prem Gateway
  August 2025+.

## Connect (Python)

```python
import deltaforge_adbc as df

conn = df.connect(
    control_plane="https://control.example.com",
    token="df_pat_...",                    # personal access token
    compute="https://compute.example.com", # optional; auto-selected if omitted
)
```

Parameters also fall back to environment variables:
`DELTAFORGE_CONTROL_PLANE_URL`, `DELTAFORGE_SESSION_TOKEN`,
`DELTAFORGE_COMPUTE_URL`.

## Read

```python
table = df.read_table(conn, "SELECT * FROM sales.public.orders LIMIT 1000")
pdf   = table.to_pandas()
```

## Write a DataFrame

```python
import pandas as pd
frame = pd.DataFrame({"id": [1, 2, 3], "region": ["us", "eu", "us"], "qty": [10, 20, 30]})

df.write_dataframe(conn, "sales.public.orders", frame, mode="append")
# mode also accepts "replace" (overwrite) and "upsert".
# idempotency_key="..." makes a re-sent batch a no-op instead of a duplicate.
```

pandas, polars, and pyarrow inputs are accepted; the target table must already
exist.

## Examples and tests

- [`examples/python/`](examples/python/): `read.py` and `write_dataframe.py`,
  self-contained scripts driven by the environment variables above.
- [`tests/`](tests/): client tests you can run against the installed driver,
  covering append, overwrite, upsert, idempotent replay, schema coercion, and
  concurrent writes, in Python ([`tests/python/`](tests/python/)) and .NET
  ([`tests/dotnet/`](tests/dotnet/)). Each test skips cleanly when the
  connection environment variables are unset. See [`tests/README.md`](tests/README.md).

## Driver options (native / ADBC client)

When loading the shared library directly, set these database options:

| Option | Meaning |
| --- | --- |
| `driver` | path to `libdeltaforge_adbc.{so,dylib,dll}` |
| `entrypoint` | `AdbcDriverInit` |
| `uri` | control-plane URL |
| `adbc.deltaforge.session_token` | session token / PAT (`df_...` / `df_pat_...`) |
| `adbc.deltaforge.compute_url` | optional compute-node URL |

Bulk writes use `cursor.adbc_ingest(table, batch, mode=...)`; `upsert`,
idempotency keys, and landing options ride on the `df.ingest.*` statement
options.

## Support

Issues and questions:
[github.com/deltaforge-org/delta-forge-adbc/issues](https://github.com/deltaforge-org/delta-forge-adbc/issues).
Docs: [deltaforge.org/docs](https://deltaforge.org/docs).
