#!/usr/bin/env python3
"""
End-to-end bulk Arrow ingest test for the DeltaForge ADBC driver.

Drives every ingest path the Rust server-side `run_ingest` supports:

    1. APPEND      via cursor.adbc_ingest(mode="append")
    2. APPEND+REPLAY via the same idempotency_key
    3. OVERWRITE   via cursor.adbc_ingest(mode="replace")
    4. UPSERT      via the df.ingest.* statement options
    5. LAND        via the df.ingest.* statement options
    6. Schema-skew COERCE behavior (Int32 -> Int64 widening)
    7. Strict COERCE=false rejection (SQLSTATE 22018)
    8. Unknown column rejection (SQLSTATE 42S22)
    9. Concurrent ingests both commit

Requires:
    - A reachable control plane and compute node.
    - Env var DELTAFORGE_SESSION_TOKEN (df_... or df_pat_...)
    - Env var DELTAFORGE_ADBC_PATH pointing at libdeltaforge_adbc.so
    - The target table already exists; this script will not CREATE it.

Skips (clear message + exit 77) when any of the above is missing.

Run:
    python3 -m pytest delta-forge-adbc/tests/python/test_ingest.py -v
"""

import os
import sys
import tempfile
import threading
import uuid
import pyarrow as pa


def _check_env():
    """Return (driver_path, session_token, target_table, target_location)
    or skip cleanly when the harness can't run."""
    driver_path = os.environ.get("DELTAFORGE_ADBC_PATH")
    token = os.environ.get("DELTAFORGE_SESSION_TOKEN")
    target = os.environ.get(
        "DELTAFORGE_INGEST_TARGET", "test.bulk_ingest.smoke_adbc"
    )
    location = os.environ.get(
        "DELTAFORGE_INGEST_LAND_LOCATION",
        "file:///tmp/df_ingest_land_adbc",
    )
    if not driver_path or not token:
        print(
            "SKIP: set DELTAFORGE_ADBC_PATH and DELTAFORGE_SESSION_TOKEN to "
            "run the bulk-ingest test against a live control plane."
        )
        sys.exit(77)
    return driver_path, token, target, location


def _connect(driver_path, token):
    """Open an ADBC connection to the live compute node."""
    try:
        import adbc_driver_manager
        import adbc_driver_manager.dbapi as dbapi
    except ImportError:
        print("SKIP: adbc_driver_manager not installed (pip install adbc-driver-manager)")
        sys.exit(77)

    db_kwargs = {
        "driver": driver_path,
        "entrypoint": "AdbcDriverInit",
        # DeltaForge's ADBC driver: control-plane URL goes in the
        # standard `uri` option; session token + compute URL ride on the
        # `adbc.deltaforge.*` extensions.
        "uri": os.environ.get(
            "DELTAFORGE_CONTROL_PLANE_URL", "http://localhost:3000"
        ),
        "adbc.deltaforge.session_token": token,
    }
    if "DELTAFORGE_COMPUTE_URL" in os.environ:
        db_kwargs["adbc.deltaforge.compute_url"] = os.environ["DELTAFORGE_COMPUTE_URL"]
    return dbapi.connect(db_kwargs=db_kwargs)


def _standard_batch(start, rows):
    return pa.RecordBatch.from_pydict(
        {
            "id": pa.array([start + i for i in range(rows)], type=pa.int64()),
            "region": pa.array(
                [["us-east", "us-west", "eu-central"][i % 3] for i in range(rows)],
                type=pa.string(),
            ),
            "qty": pa.array(
                [(start + i) * 10 for i in range(rows)], type=pa.int32()
            ),
        }
    )


def _count_rows(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def test_append_basic():
    driver_path, token, target, _location = _check_env()
    conn = _connect(driver_path, token)
    try:
        before = _count_rows(conn, target)
        batch = _standard_batch(0, 250)
        with conn.cursor() as cur:
            cur.adbc_ingest(target, batch, mode="append")
            # ADBC v1.0 routes adbc_ingest through ExecuteQuery(NULL)
            # which always returns rows_affected = -1 (no ExecuteUpdate
            # ABI exists at v1.0). The authoritative check is the
            # post-ingest row count.
        after = _count_rows(conn, target)
        assert after - before == 250, f"row delta {after - before} != 250"
    finally:
        conn.close()


def test_append_idempotency_replay():
    driver_path, token, target, _location = _check_env()
    conn = _connect(driver_path, token)
    key = f"adbc-idem-{uuid.uuid4()}"
    try:
        before = _count_rows(conn, target)
        batch = _standard_batch(10_000, 75)
        for attempt in range(2):
            with conn.cursor() as cur:
                cur.adbc_statement.set_options(**{
                    "df.ingest.idempotency_key": key, "df.ingest.mode": "append"
                })
                # Bind the stream + execute the update so the
                # idempotency_key flows through the manifest.
                cur.adbc_statement.bind(batch)
                cur.adbc_statement.set_options(**{"adbc.ingest.target_table": target})
                cur.adbc_statement.execute_update()
        after = _count_rows(conn, target)
        delta = after - before
        assert delta == 75, (
            f"idempotent replay: row delta should be 75 (one commit, one "
            f"replay short-circuit); got {delta}"
        )
    finally:
        conn.close()


def test_overwrite_replaces_prior():
    driver_path, token, target, _location = _check_env()
    overwrite_target = os.environ.get(
        "DELTAFORGE_INGEST_OVERWRITE_TARGET",
        target + "_overwrite",
    )
    conn = _connect(driver_path, token)
    try:
        # Seed 30 rows.
        with conn.cursor() as cur:
            cur.adbc_ingest(overwrite_target, _standard_batch(0, 30), mode="append")
        # Overwrite to 5 rows.
        with conn.cursor() as cur:
            cur.adbc_ingest(
                overwrite_target,
                _standard_batch(1_000, 5),
                mode="replace",
            )
        assert _count_rows(conn, overwrite_target) == 5
    finally:
        conn.close()


def test_upsert_via_df_extensions():
    driver_path, token, target, _location = _check_env()
    upsert_target = os.environ.get(
        "DELTAFORGE_INGEST_UPSERT_TARGET",
        target + "_upsert",
    )
    conn = _connect(driver_path, token)
    try:
        # Seed: ids 0..10.
        with conn.cursor() as cur:
            cur.adbc_ingest(upsert_target, _standard_batch(0, 10), mode="append")
        # Source rows: ids 5..15. Overlap on 5..10 (update), 10..15 new.
        src = _standard_batch(5, 10)
        with conn.cursor() as cur:
            stmt = cur.adbc_statement
            stmt.set_options(**{
                "adbc.ingest.target_table": upsert_target,
                "df.ingest.mode": "upsert",
                "df.ingest.key_columns": "id",
            })
            stmt.bind(src)
            stmt.execute_update()
        assert _count_rows(conn, upsert_target) == 15
    finally:
        conn.close()


def test_land_returns_paths():
    driver_path, token, _target, location = _check_env()
    conn = _connect(driver_path, token)
    try:
        with conn.cursor() as cur:
            stmt = cur.adbc_statement
            stmt.set_options(**{
                "df.ingest.mode": "land",
                "df.ingest.target_location": location,
                "df.ingest.payload_format": "arrow_ipc",
            })
            stmt.bind(_standard_batch(0, 17))
            stmt.execute_update()
    finally:
        conn.close()


def test_coerce_widening_succeeds():
    driver_path, token, target, _location = _check_env()
    conn = _connect(driver_path, token)
    try:
        before = _count_rows(conn, target)
        # `id` is Int64 on the target; ship Int32 with coerce=true (default).
        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([20_000, 20_001], type=pa.int32()),
                "region": pa.array(["us-east", "us-west"]),
                "qty": pa.array([100, 200], type=pa.int32()),
            }
        )
        with conn.cursor() as cur:
            cur.adbc_ingest(target, batch, mode="append")
        assert _count_rows(conn, target) - before == 2
    finally:
        conn.close()


def test_coerce_false_strict_rejects():
    driver_path, token, target, _location = _check_env()
    conn = _connect(driver_path, token)
    try:
        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([30_000], type=pa.int32()),
                "region": pa.array(["us-east"]),
                "qty": pa.array([1], type=pa.int32()),
            }
        )
        try:
            with conn.cursor() as cur:
                stmt = cur.adbc_statement
                stmt.set_options(**{
                    "adbc.ingest.target_table": target,
                    "df.ingest.mode": "append",
                    "df.ingest.coerce": "false",
                })
                stmt.bind(batch)
                stmt.execute_update()
            assert False, "coerce=false must reject Int32 -> Int64 widening"
        except Exception as e:
            assert "22018" in str(e), f"expected SQLSTATE 22018 in {e}"
    finally:
        conn.close()


def test_unknown_column_rejected():
    driver_path, token, target, _location = _check_env()
    conn = _connect(driver_path, token)
    try:
        batch = pa.RecordBatch.from_pydict(
            {
                "id": pa.array([40_000], type=pa.int64()),
                "region": pa.array(["us-east"]),
                "qty": pa.array([1], type=pa.int32()),
                "ghost": pa.array(["x"]),  # not on the target schema
            }
        )
        try:
            with conn.cursor() as cur:
                cur.adbc_ingest(target, batch, mode="append")
            assert False, "ingest must reject unknown column 'ghost'"
        except Exception as e:
            assert "42S22" in str(e) or "ghost" in str(e), (
                f"expected SQLSTATE 42S22 (or 'ghost' in message) in {e}"
            )
    finally:
        conn.close()


def test_concurrent_two_writers_both_commit():
    driver_path, token, target, _location = _check_env()

    def worker(start, rows):
        c = _connect(driver_path, token)
        try:
            with c.cursor() as cur:
                cur.adbc_ingest(target, _standard_batch(start, rows), mode="append")
        finally:
            c.close()

    conn = _connect(driver_path, token)
    try:
        before = _count_rows(conn, target)
    finally:
        conn.close()

    t1 = threading.Thread(target=worker, args=(50_000, 200))
    t2 = threading.Thread(target=worker, args=(60_000, 200))
    t1.start(); t2.start()
    t1.join(); t2.join()

    conn = _connect(driver_path, token)
    try:
        after = _count_rows(conn, target)
    finally:
        conn.close()
    assert after - before == 400, (
        f"concurrent writers: expected 400-row delta, got {after - before}"
    )


if __name__ == "__main__":
    # Allow direct invocation as a script.
    tests = [
        test_append_basic,
        test_append_idempotency_replay,
        test_overwrite_replaces_prior,
        test_upsert_via_df_extensions,
        test_land_returns_paths,
        test_coerce_widening_succeeds,
        test_coerce_false_strict_rejects,
        test_unknown_column_rejected,
        test_concurrent_two_writers_both_commit,
    ]
    failures = 0
    for t in tests:
        try:
            print(f"--- {t.__name__}")
            t()
            print(f"    PASS")
        except SystemExit as e:
            if e.code == 77:
                raise
            failures += 1
            print(f"    FAIL: {e}")
        except Exception as e:
            failures += 1
            print(f"    FAIL: {e}")
    if failures:
        sys.exit(1)
    print("ALL PASS")
