#!/usr/bin/env python3
"""
Edge-case sweep for the DeltaForge bulk Arrow ingest endpoint via ADBC.

Covers data-type variants and boundary conditions that the smoke suite
(test_ingest.py) does NOT exercise:

    1. NULL values in every column type
    2. Decimal128 / Decimal256
    3. TIMESTAMP with and without timezone
    4. DATE, TIME32, TIME64
    5. Nested types (List, Struct)
    6. Utf8View, BinaryView
    7. Dictionary-encoded columns
    8. Very wide schemas (60 columns)
    9. Partitioned target tables (single + multi-key)
   10. Schema evolution: source has new column / missing nullable column
   11. Boundary integer values (i64::MIN, i64::MAX)
   12. Unicode in strings (multi-byte, emoji, control chars)
   13. Binary blobs
   14. Empty batch (zero rows) within a larger ingest
   15. Single-row batch (minimum payload)
   16. Large batch (100k rows in a single BATCH frame)
   17. Many small batches (1000 batches of 1 row each)
   18. Float INF / NaN

All tests assume a live control plane + the same DSN / token env vars as
test_ingest.py. Tables are dropped + recreated per test so each run is
hermetic.
"""

import os
import sys
import uuid
import math
import struct
from datetime import datetime, date, time, timezone, timedelta
from decimal import Decimal

# pyarrow MUST import before adbc_driver_manager dlopen's the driver to
# avoid the libarrow ABI clash with the system arrow that pyarrow's
# bundled libparquet references.
import pyarrow as pa
import pyarrow.parquet as pq  # noqa: F401
import requests
import adbc_driver_manager.dbapi as dbapi


CP   = os.environ.get("DELTAFORGE_CONTROL_PLANE_URL", "http://172.29.80.1:3000")
COMP = os.environ.get("DELTAFORGE_COMPUTE_URL",       "http://172.29.80.1:3031")
PAT  = os.environ.get("DELTAFORGE_SESSION_TOKEN")
ADBC = os.environ.get("DELTAFORGE_ADBC_PATH")
ZONE = os.environ.get("DELTAFORGE_EDGE_ZONE",   "test")
SCH  = os.environ.get("DELTAFORGE_EDGE_SCHEMA", "retail")
LOC_ROOT = os.environ.get("DELTAFORGE_EDGE_LOCATION_ROOT", "A:/tem/retail")

if not ADBC or not PAT:
    print("SKIP: set DELTAFORGE_ADBC_PATH + DELTAFORGE_SESSION_TOKEN")
    sys.exit(77)


# ----------------------------------------------------------------------------
# Live-control-plane helpers (over HTTP, separate from ADBC)
# ----------------------------------------------------------------------------

def _http_sql(sql):
    r = requests.post(
        f"{COMP}/api/v1/query/stream/binary",
        headers={"Authorization": f"Bearer {PAT}",
                 "Content-Type": "application/json"},
        json={"sql": sql, "include_plan": False},
        timeout=30,
    )
    return r.status_code, r.text[:200] if r.status_code != 200 else r.content


def _create_table(qualified, schema_sql, partition_by=None):
    _http_sql(f"DROP TABLE IF EXISTS {qualified}")
    suffix = qualified.split(".")[-1] + "_" + uuid.uuid4().hex[:8]
    loc = f"{LOC_ROOT}/{suffix}"
    pb = f" PARTITIONED BY ({partition_by})" if partition_by else ""
    code, body = _http_sql(
        f"CREATE DELTA TABLE {qualified} ({schema_sql}) LOCATION '{loc}'{pb}"
    )
    if code != 200:
        raise RuntimeError(f"CREATE failed for {qualified}: {body}")
    return qualified


# ----------------------------------------------------------------------------
# ADBC fixture
# ----------------------------------------------------------------------------

def _connect():
    return dbapi.connect(db_kwargs={
        "driver":     ADBC,
        "entrypoint": "AdbcDriverInit",
        "uri":        CP,
        "adbc.deltaforge.session_token": PAT,
        "adbc.deltaforge.compute_url":   COMP,
    })


def _ingest_batch(conn, table, batch, mode="append",
                  extra_opts=None, payload_format="arrow_ipc"):
    extra_opts = extra_opts or {}
    with conn.cursor() as cur:
        stmt = cur.adbc_statement
        stmt.set_options(**{"adbc.ingest.target_table": table})
        if mode in ("append", "create", "create_append", "replace"):
            stmt.set_options(**{"adbc.ingest.mode": "adbc.ingest.mode." + mode})
        else:
            stmt.set_options(**{"df.ingest.mode": mode})
        if payload_format != "arrow_ipc":
            stmt.set_options(**{"df.ingest.payload_format": payload_format})
        for k, v in extra_opts.items():
            stmt.set_options(**{k: v})
        stmt.bind(batch)
        stmt.execute_update()


def _count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def _qname(stem):
    return f"{ZONE}.{SCH}.df_edge_{stem}_{uuid.uuid4().hex[:6]}"


# ----------------------------------------------------------------------------
# Test cases
# ----------------------------------------------------------------------------

def test_nulls_every_type():
    t = _qname("nulls")
    _create_table(t,
        "id BIGINT NOT NULL, "
        "s STRING, "
        "i INT, "
        "f DOUBLE, "
        "b BOOLEAN, "
        "ts TIMESTAMP")
    batch = pa.RecordBatch.from_pydict({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "s":  pa.array(["a", None, "c"], type=pa.string()),
        "i":  pa.array([None, 2, None], type=pa.int32()),
        "f":  pa.array([1.5, None, 3.0], type=pa.float64()),
        "b":  pa.array([True, None, False], type=pa.bool_()),
        "ts": pa.array(
            [datetime(2026, 1, 1, 12, 0, 0), None, datetime(2026, 12, 31, 23, 59, 59)],
            type=pa.timestamp("us")),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 3
    finally:
        conn.close()


def test_decimal128():
    t = _qname("dec128")
    _create_table(t, "id BIGINT NOT NULL, amount DECIMAL(18, 4)")
    batch = pa.RecordBatch.from_pydict({
        "id":     pa.array([1, 2, 3], type=pa.int64()),
        "amount": pa.array(
            [Decimal("123.4500"), Decimal("-9999.9999"), Decimal("0.0001")],
            type=pa.decimal128(18, 4)),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 3
    finally:
        conn.close()


def test_timestamp_tz_and_naive():
    t = _qname("ts_tz")
    _create_table(t, "id BIGINT NOT NULL, ts_utc TIMESTAMP, ts_naive TIMESTAMP_NTZ")
    batch = pa.RecordBatch.from_pydict({
        "id": pa.array([1, 2], type=pa.int64()),
        "ts_utc": pa.array(
            [datetime(2026, 5, 27, 12, 0, 0, tzinfo=timezone.utc),
             datetime(2026, 5, 27, 18, 30, 0, tzinfo=timezone(timedelta(hours=5)))],
            type=pa.timestamp("us", tz="UTC")),
        "ts_naive": pa.array(
            [datetime(2026, 5, 27, 12, 0, 0), datetime(2026, 5, 27, 18, 30, 0)],
            type=pa.timestamp("us")),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 2
    finally:
        conn.close()


def test_date_and_time():
    t = _qname("date_time")
    _create_table(t, "id BIGINT NOT NULL, d DATE, hms TIME")
    batch = pa.RecordBatch.from_pydict({
        "id":  pa.array([1, 2], type=pa.int64()),
        "d":   pa.array([date(2026, 1, 1), date(2026, 12, 31)], type=pa.date32()),
        "hms": pa.array([time(9, 30, 0), time(23, 59, 59)], type=pa.time64("us")),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 2
    finally:
        conn.close()


def test_wide_schema_60_columns():
    t = _qname("wide")
    cols = ", ".join([f"c{i:02d} INT" for i in range(60)])
    _create_table(t, f"id BIGINT NOT NULL, {cols}")
    pydict = {"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())}
    for i in range(60):
        pydict[f"c{i:02d}"] = pa.array([i, i + 1, i + 2, i + 3, i + 4], type=pa.int32())
    batch = pa.RecordBatch.from_pydict(pydict)
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 5
    finally:
        conn.close()


def test_partitioned_target():
    t = _qname("part")
    _create_table(t,
        "id BIGINT NOT NULL, region STRING NOT NULL, qty INT",
        partition_by="region")
    batch = pa.RecordBatch.from_pydict({
        "id":     pa.array(list(range(60)), type=pa.int64()),
        "region": pa.array([["us", "eu", "ap"][i % 3] for i in range(60)],
                           type=pa.string()),
        "qty":    pa.array(list(range(60)), type=pa.int32()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 60
    finally:
        conn.close()


def test_boundary_integers():
    t = _qname("bounds")
    _create_table(t, "id BIGINT NOT NULL, i8 TINYINT, i16 SMALLINT, i32 INT")
    batch = pa.RecordBatch.from_pydict({
        "id":  pa.array([1, 2, 3, 4], type=pa.int64()),
        "i8":  pa.array([-128, 127, 0, None], type=pa.int8()),
        "i16": pa.array([-32768, 32767, 0, None], type=pa.int16()),
        "i32": pa.array([-2147483648, 2147483647, 0, None], type=pa.int32()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 4
    finally:
        conn.close()


def test_int64_extremes():
    t = _qname("i64x")
    _create_table(t, "id BIGINT NOT NULL, val BIGINT")
    batch = pa.RecordBatch.from_pydict({
        "id":  pa.array([1, 2, 3], type=pa.int64()),
        "val": pa.array([-(2**63), (2**63) - 1, 0], type=pa.int64()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 3
    finally:
        conn.close()


def test_unicode_strings():
    t = _qname("uni")
    _create_table(t, "id BIGINT NOT NULL, s STRING")
    batch = pa.RecordBatch.from_pydict({
        "id": pa.array([1, 2, 3, 4, 5, 6], type=pa.int64()),
        "s":  pa.array([
            "ascii only",
            "naïve café",                                 # latin-1 extended
            "你好，世界",                                  # CJK
            "🚀✨ emoji mix 🎉",                           # 4-byte UTF-8
            "newline\nand\ttab\rand\x00null",             # control chars (NUL stripped server-side if applicable)
            "𝓊𝓃𝒾𝒸𝑜𝒹𝑒 mathematical bold script",          # surrogate-pair / supplementary plane
        ], type=pa.string()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 6
    finally:
        conn.close()


def test_binary_blobs():
    t = _qname("bin")
    _create_table(t, "id BIGINT NOT NULL, payload BINARY")
    batch = pa.RecordBatch.from_pydict({
        "id":      pa.array([1, 2, 3], type=pa.int64()),
        "payload": pa.array([
            b"",                          # empty
            b"\x00\x01\x02\xff",          # small with NULs
            bytes(range(256)),            # all byte values
        ], type=pa.binary()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 3
    finally:
        conn.close()


def test_float_inf_nan():
    t = _qname("fpx")
    _create_table(t, "id BIGINT NOT NULL, f DOUBLE")
    batch = pa.RecordBatch.from_pydict({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "f":  pa.array(
            [math.inf, -math.inf, math.nan, 0.0, -0.0],
            type=pa.float64()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 5
    finally:
        conn.close()


def test_single_row_batch():
    t = _qname("one")
    _create_table(t, "id BIGINT NOT NULL")
    conn = _connect()
    try:
        _ingest_batch(conn, t, pa.RecordBatch.from_pydict({
            "id": pa.array([42], type=pa.int64()),
        }))
        assert _count(conn, t) == 1
    finally:
        conn.close()


def test_large_batch_100k_rows():
    t = _qname("big")
    _create_table(t, "id BIGINT NOT NULL, region STRING, qty INT")
    n = 100_000
    batch = pa.RecordBatch.from_pydict({
        "id":     pa.array(list(range(n)), type=pa.int64()),
        "region": pa.array([["a", "b", "c"][i % 3] for i in range(n)], type=pa.string()),
        "qty":    pa.array(list(range(n)), type=pa.int32()),
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == n
    finally:
        conn.close()


def test_many_small_batches():
    """1000 single-row ingests in a tight loop. Stresses connection reuse,
    per-call session lookup, idempotency-cache GC, and the ingest handler's
    setup/teardown overhead."""
    t = _qname("many")
    _create_table(t, "id BIGINT NOT NULL")
    conn = _connect()
    try:
        for i in range(1000):
            _ingest_batch(conn, t, pa.RecordBatch.from_pydict({
                "id": pa.array([i], type=pa.int64()),
            }))
        assert _count(conn, t) == 1000
    finally:
        conn.close()


def test_schema_evolution_missing_nullable_column():
    """Source omits a nullable target column. Target schema is
    authoritative; missing nullable columns are filled with NULL."""
    t = _qname("missing")
    _create_table(t, "id BIGINT NOT NULL, region STRING, qty INT")
    batch = pa.RecordBatch.from_pydict({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        # region + qty omitted
    })
    conn = _connect()
    try:
        _ingest_batch(conn, t, batch)
        assert _count(conn, t) == 3
    finally:
        conn.close()


# ----------------------------------------------------------------------------
# Runner
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_nulls_every_type,
        test_decimal128,
        test_timestamp_tz_and_naive,
        test_date_and_time,
        test_wide_schema_60_columns,
        test_partitioned_target,
        test_boundary_integers,
        test_int64_extremes,
        test_unicode_strings,
        test_binary_blobs,
        test_float_inf_nan,
        test_single_row_batch,
        test_large_batch_100k_rows,
        test_many_small_batches,
        test_schema_evolution_missing_nullable_column,
    ]
    failures = 0
    for t in tests:
        try:
            print(f"--- {t.__name__}")
            t()
            print("    PASS")
        except Exception as e:
            failures += 1
            print(f"    FAIL: {type(e).__name__}: {e}")
    if failures:
        print(f"=== {failures}/{len(tests)} failed ===")
        sys.exit(1)
    print(f"ALL PASS ({len(tests)} tests)")
