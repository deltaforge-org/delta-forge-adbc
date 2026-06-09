#!/usr/bin/env python3
"""Read from a Delta table with the DeltaForge ADBC driver.

Set these environment variables before running:
    DELTAFORGE_CONTROL_PLANE_URL   e.g. https://control.example.com
    DELTAFORGE_SESSION_TOKEN       df_... or df_pat_...
    DELTAFORGE_COMPUTE_URL         optional; auto-selected if omitted

Usage:
    python read.py "SELECT * FROM sales.public.orders LIMIT 100"
"""

import sys
import deltaforge_adbc as df


def main() -> int:
    sql = sys.argv[1] if len(sys.argv) > 1 else "SELECT 1 AS one"
    conn = df.connect()
    try:
        table = df.read_table(conn, sql)
        print(f"{table.num_rows} rows x {table.num_columns} columns")
        print(table.to_pandas().head(20).to_string(index=False))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
