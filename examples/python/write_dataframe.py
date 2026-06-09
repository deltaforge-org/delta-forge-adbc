#!/usr/bin/env python3
"""Write a pandas DataFrame into a Delta table with the DeltaForge ADBC driver.

Set these environment variables before running:
    DELTAFORGE_CONTROL_PLANE_URL   e.g. https://control.example.com
    DELTAFORGE_SESSION_TOKEN       df_... or df_pat_...
    DELTAFORGE_COMPUTE_URL         optional; auto-selected if omitted
    DELTAFORGE_INGEST_TARGET       target table, e.g. sales.public.orders
                                   (must already exist)

Usage:
    python write_dataframe.py            # appends a small sample frame
    python write_dataframe.py replace    # overwrites instead of appending
"""

import os
import sys

import pandas as pd

import deltaforge_adbc as df


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "append"
    target = os.environ.get("DELTAFORGE_INGEST_TARGET")
    if not target:
        print("set DELTAFORGE_INGEST_TARGET to a fully qualified existing table")
        return 2

    frame = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "region": ["us-east", "us-west", "eu-central", "us-east"],
            "qty": [10, 20, 30, 40],
        }
    )

    conn = df.connect()
    try:
        before = df.read_table(conn, f"SELECT COUNT(*) AS n FROM {target}").to_pydict()["n"][0]
        df.write_dataframe(conn, target, frame, mode=mode)
        after = df.read_table(conn, f"SELECT COUNT(*) AS n FROM {target}").to_pydict()["n"][0]
        print(f"{target}: {before} -> {after} rows (mode={mode})")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
