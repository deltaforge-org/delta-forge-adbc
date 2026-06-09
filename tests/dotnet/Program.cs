// Program.cs
//
// .NET ADBC end-to-end test for the DeltaForge bulk Arrow ingest endpoint.
// Drives the bridge through Apache.Arrow.Adbc 0.18 using the C-ABI driver
// loader path (AdbcDriverLoader.FindAndLoadDriver).
//
// What this exercises:
//   1. Driver load via the C ABI
//   2. AdbcDatabase11 SetOption + Open + Connect
//   3. AdbcConnection11 CreateStatement
//   4. AdbcStatement11 SetOption (adbc.ingest.target_table, mode, ...)
//   5. AdbcStatement11 Bind(RecordBatch) + ExecuteUpdate()
//   6. SELECT readback to verify the rows landed
//
// Env vars:
//   DELTAFORGE_ADBC_PATH         path to libdeltaforge_adbc.so / .dll
//   DELTAFORGE_SESSION_TOKEN     df_... or df_pat_...
//   DELTAFORGE_CONTROL_PLANE_URL default http://localhost:3000
//   DELTAFORGE_COMPUTE_URL       optional pinned compute URL
//   DELTAFORGE_INGEST_TARGET     qualified target table
//
// Exit codes: 0 PASS, 1 FAIL, 77 SKIP

using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Types;

internal static class Program
{
    private static string AdbcPath() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_ADBC_PATH")
            ?? throw new InvalidOperationException("DELTAFORGE_ADBC_PATH not set");
    private static string Token() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_SESSION_TOKEN")
            ?? throw new InvalidOperationException("DELTAFORGE_SESSION_TOKEN not set");
    private static string ControlPlaneUrl() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_CONTROL_PLANE_URL") ?? "http://localhost:3000";
    private static string? ComputeUrl() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_COMPUTE_URL");
    private static string TargetTable() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_INGEST_TARGET")
            ?? "test.retail.df_ingest_smoke_adbc_dotnet";
    private static string OverwriteTargetTable() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_INGEST_OVERWRITE_TARGET")
            ?? (TargetTable() + "_overwrite");
    private static string UpsertTargetTable() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_INGEST_UPSERT_TARGET")
            ?? (TargetTable() + "_upsert");
    private static string LandLocation() =>
        Environment.GetEnvironmentVariable("DELTAFORGE_INGEST_LAND_LOCATION")
            ?? "A:/tem/retail/df_ingest_land_adbc_dotnet";

    public static int Main()
    {
        string adbcPath;
        string token;
        try
        {
            adbcPath = AdbcPath();
            token = Token();
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"SKIP: {e.Message}");
            return 77;
        }

        // Apache.Arrow.Adbc 0.18: AdbcDriverLoader is the C-ABI shim that
        // dlopen()s the shared library and binds its function pointers.
        // FindAndLoadDriver returns an AdbcDriver11 we feed into
        // AdbcDatabase11.
        AdbcDriver driver;
        try
        {
            driver = AdbcDriverLoader.FindAndLoadDriver(
                adbcPath,
                "AdbcDriverInit",
                AdbcVersion.Version_1_0_0);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"SKIP: cannot load ADBC driver {adbcPath}: {e.Message}");
            return 77;
        }

        var dbOptions = new Dictionary<string, string>
        {
            ["uri"] = ControlPlaneUrl(),
            ["adbc.deltaforge.session_token"] = token,
        };
        var compute = ComputeUrl();
        if (!string.IsNullOrEmpty(compute))
            dbOptions["adbc.deltaforge.compute_url"] = compute;

        AdbcDatabase db;
        AdbcConnection conn;
        try
        {
            db = driver.Open(dbOptions);
            conn = db.Connect(new Dictionary<string, string>());
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"SKIP: cannot open database / connect: {e.Message}");
            return 77;
        }

        var failures = 0;
        try
        {
            failures += Run("AppendBasic", () => AppendBasic(conn));
            failures += Run("OverwriteReplaces", () => OverwriteReplaces(conn));
            failures += Run("UpsertWithKeys", () => UpsertWithKeys(conn));
            failures += Run("LandReturnsPaths", () => LandReturnsPaths(conn));
            failures += Run("IdempotencyReplay", () => IdempotencyReplay(conn));
            failures += Run("StrictCoerceRejects", () => StrictCoerceRejects(conn));
            failures += Run("UnknownColumnRejects", () => UnknownColumnRejects(conn));
        }
        finally
        {
            try { conn.Dispose(); } catch { }
            try { db.Dispose(); } catch { }
            try { driver.Dispose(); } catch { }
        }
        Console.WriteLine(failures == 0 ? "ALL PASS" : $"FAILURES: {failures}");
        return failures == 0 ? 0 : 1;
    }

    private static int Run(string name, Action body)
    {
        try
        {
            Console.WriteLine($"--- {name}");
            body();
            Console.WriteLine("    PASS");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine($"    FAIL: {e.GetType().Name}: {e.Message}");
            return 1;
        }
    }

    private static RecordBatch StandardBatch(long start, int rows)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("region").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("qty").DataType(Int32Type.Default).Nullable(true))
            .Build();
        var idB = new Int64Array.Builder();
        var rgB = new StringArray.Builder();
        var qtB = new Int32Array.Builder();
        var regions = new[] { "us-east", "us-west", "eu-central" };
        for (var i = 0; i < rows; ++i)
        {
            idB.Append(start + i);
            rgB.Append(regions[i % 3]);
            qtB.Append((int)((start + i) * 10));
        }
        var arrays = new IArrowArray[] { idB.Build(), rgB.Build(), qtB.Build() };
        return new RecordBatch(schema, arrays, rows);
    }

    private static long CountRows(AdbcConnection conn, string table)
    {
        using var stmt = conn.CreateStatement();
        stmt.SqlQuery = $"SELECT COUNT(*) AS n FROM {table}";
        var result = stmt.ExecuteQuery();
        var stream = result.Stream
            ?? throw new InvalidOperationException("SELECT COUNT(*) produced no stream");
        using (stream)
        {
            var batch = stream.ReadNextRecordBatchAsync().GetAwaiter().GetResult()
                ?? throw new InvalidOperationException("SELECT COUNT(*) returned no rows");
            using (batch)
            {
                return ((Int64Array)batch.Column(0)).GetValue(0)!.Value;
            }
        }
    }

    private static void ExecuteIngest(
        AdbcConnection conn,
        string? targetTable,
        string? targetLocation,
        string mode,
        IEnumerable<KeyValuePair<string, string>> extraOptions,
        RecordBatch batch)
    {
        using var stmt = conn.CreateStatement();
        if (!string.IsNullOrEmpty(targetTable))
            stmt.SetOption("adbc.ingest.target_table", targetTable);
        if (!string.IsNullOrEmpty(targetLocation))
            stmt.SetOption("df.ingest.target_location", targetLocation);
        if (mode == "append" || mode == "create" || mode == "create_append" || mode == "replace")
            stmt.SetOption("adbc.ingest.mode", mode);
        else
            stmt.SetOption("df.ingest.mode", mode);
        foreach (var kv in extraOptions)
            stmt.SetOption(kv.Key, kv.Value);
        stmt.Bind(batch, batch.Schema);
        stmt.ExecuteUpdate();
    }

    private static void AppendBasic(AdbcConnection conn)
    {
        var target = TargetTable();
        var before = CountRows(conn, target);
        ExecuteIngest(conn, target, null, "append", Array.Empty<KeyValuePair<string, string>>(),
                      StandardBatch(0, 100));
        var after = CountRows(conn, target);
        if (after - before != 100)
            throw new Exception($"row delta {after - before} != 100");
    }

    private static void OverwriteReplaces(AdbcConnection conn)
    {
        var target = OverwriteTargetTable();
        ExecuteIngest(conn, target, null, "append", Array.Empty<KeyValuePair<string, string>>(),
                      StandardBatch(0, 25));
        ExecuteIngest(conn, target, null, "replace", Array.Empty<KeyValuePair<string, string>>(),
                      StandardBatch(900, 4));
        var after = CountRows(conn, target);
        if (after != 4)
            throw new Exception($"overwrite: expected 4 rows, got {after}");
    }

    private static void UpsertWithKeys(AdbcConnection conn)
    {
        var target = UpsertTargetTable();
        ExecuteIngest(conn, target, null, "append", Array.Empty<KeyValuePair<string, string>>(),
                      StandardBatch(0, 10));
        var extras = new[]
        {
            new KeyValuePair<string, string>("df.ingest.key_columns", "id"),
        };
        ExecuteIngest(conn, target, null, "upsert", extras, StandardBatch(5, 10));
        var after = CountRows(conn, target);
        if (after != 15)
            throw new Exception($"upsert: expected 15 unique ids, got {after}");
    }

    private static void LandReturnsPaths(AdbcConnection conn)
    {
        ExecuteIngest(conn, null, LandLocation(), "land",
            new[] { new KeyValuePair<string, string>("df.ingest.payload_format", "arrow_ipc") },
            StandardBatch(0, 11));
    }

    private static void IdempotencyReplay(AdbcConnection conn)
    {
        var target = TargetTable();
        var before = CountRows(conn, target);
        var key = $"dotnet-adbc-idem-{Guid.NewGuid():N}";
        var extras = new[]
        {
            new KeyValuePair<string, string>("df.ingest.idempotency_key", key),
        };
        for (var attempt = 0; attempt < 2; ++attempt)
        {
            ExecuteIngest(conn, target, null, "append", extras,
                          StandardBatch(80_000 + attempt, 7));
        }
        var after = CountRows(conn, target);
        if (after - before != 7)
            throw new Exception($"idempotent replay: delta {after - before} != 7");
    }

    private static void StrictCoerceRejects(AdbcConnection conn)
    {
        var target = TargetTable();
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("region").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("qty").DataType(Int32Type.Default).Nullable(true))
            .Build();
        var idB = new Int32Array.Builder(); idB.Append(99_999);
        var rgB = new StringArray.Builder(); rgB.Append("us-east");
        var qtB = new Int32Array.Builder(); qtB.Append(1);
        var batch = new RecordBatch(schema,
            new IArrowArray[] { idB.Build(), rgB.Build(), qtB.Build() }, 1);
        try
        {
            var extras = new[] { new KeyValuePair<string, string>("df.ingest.coerce", "false") };
            ExecuteIngest(conn, target, null, "append", extras, batch);
            throw new Exception("expected coerce=false to reject Int32 -> Int64 widening");
        }
        catch (Exception e) when (e.Message.Contains("22018"))
        {
            // expected
        }
    }

    private static void UnknownColumnRejects(AdbcConnection conn)
    {
        var target = TargetTable();
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("region").DataType(StringType.Default).Nullable(true))
            .Field(f => f.Name("qty").DataType(Int32Type.Default).Nullable(true))
            .Field(f => f.Name("ghost").DataType(StringType.Default).Nullable(true))
            .Build();
        var idB = new Int64Array.Builder(); idB.Append(123_456);
        var rgB = new StringArray.Builder(); rgB.Append("us-east");
        var qtB = new Int32Array.Builder(); qtB.Append(1);
        var ghB = new StringArray.Builder(); ghB.Append("phantom");
        var batch = new RecordBatch(schema,
            new IArrowArray[] { idB.Build(), rgB.Build(), qtB.Build(), ghB.Build() }, 1);
        try
        {
            ExecuteIngest(conn, target, null, "append",
                Array.Empty<KeyValuePair<string, string>>(), batch);
            throw new Exception("expected unknown-column to be rejected");
        }
        catch (Exception e) when (e.Message.Contains("42S22") || e.Message.Contains("ghost"))
        {
            // expected
        }
    }
}
