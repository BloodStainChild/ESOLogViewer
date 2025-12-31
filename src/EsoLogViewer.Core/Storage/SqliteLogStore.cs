using System.Globalization;
using System.Data.Common;
using System.Text.Json;
using EsoLogViewer.Core.Models;
using Microsoft.Data.Sqlite;

namespace EsoLogViewer.Core.Storage;

/// <summary>
/// Stores a single imported encounter log (one uploaded .log file) in its own SQLite database.
/// This makes log packages easy to delete or share.
/// </summary>
public sealed class SqliteLogStore : IStore, IAsyncDisposable
{
    private readonly string _dbPath;
    private readonly bool _ensureSchema;

    private SqliteConnection? _bulkConn;
    private SqliteTransaction? _bulkTx;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    public SqliteLogStore(string dbPath, bool ensureSchema = true)
    {
        _dbPath = dbPath;
        _ensureSchema = ensureSchema;

        if (_ensureSchema)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_dbPath) ?? ".");
            using var conn = OpenConnection();
            EnsureSchema(conn);
        }
    }

    public string DbPath => _dbPath;

    public async Task BeginBulkImportAsync(string? sourceLogFileName, CancellationToken ct = default)
    {
        if (_bulkConn is not null) return;

        _bulkConn = OpenConnection();
        await _bulkConn.OpenAsync(ct);

        if (_ensureSchema)
            EnsureSchema(_bulkConn);

        // Pragmas must be outside of a transaction.
        await ExecAsync(_bulkConn, "PRAGMA journal_mode=WAL;", ct);
        await ExecAsync(_bulkConn, "PRAGMA synchronous=NORMAL;", ct);
        await ExecAsync(_bulkConn, "PRAGMA temp_store=MEMORY;", ct);

        if (!string.IsNullOrWhiteSpace(sourceLogFileName))
        {
            await SetMetaAsync(_bulkConn, "SourceFileName", sourceLogFileName, ct);
        }
        await SetMetaAsync(_bulkConn, "ImportedAtUtc", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture), ct);

	    // BeginTransactionAsync returns DbTransaction; the underlying runtime type is SqliteTransaction.
	    _bulkTx = (SqliteTransaction)await _bulkConn.BeginTransactionAsync(ct);
    }

    public async Task EndBulkImportAsync(CancellationToken ct = default)
    {
        if (_bulkConn is null) return;

        try
        {
            if (_bulkTx is not null)
                await _bulkTx.CommitAsync(ct);
        }
        finally
        {
            await DisposeBulkAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeBulkAsync();
    }

    private async Task DisposeBulkAsync()
    {
        try
        {
            if (_bulkTx is not null)
                await _bulkTx.DisposeAsync();
        }
        finally
        {
            _bulkTx = null;
        }

        try
        {
            if (_bulkConn is not null)
                await _bulkConn.DisposeAsync();
        }
        finally
        {
            _bulkConn = null;
        }
    }

    // --------------------
    // Read API
    // --------------------

    public IReadOnlyList<SessionSummary> GetSessions()
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();

        var hasDisplayName = ColumnExists(conn, "Sessions", "DisplayName");
        var hasTrialInitKey = ColumnExists(conn, "Sessions", "TrialInitKey");

        string sql;
        if (hasTrialInitKey)
        {
            sql = hasDisplayName ? @"
SELECT Id, UnixStartTimeMs,
       COALESCE(NULLIF(DisplayName, ''), Title) AS DisplayTitle,
       Server, Language, Patch, FightCount, TrialInitKey
FROM Sessions
ORDER BY UnixStartTimeMs DESC;" : @"
SELECT Id, UnixStartTimeMs, Title, Server, Language, Patch, FightCount, TrialInitKey
FROM Sessions
ORDER BY UnixStartTimeMs DESC;";
        }
        else
        {
            // Older DBs: read the JSON detail and try to extract the TrialInitKey from there.
            sql = hasDisplayName ? @"
SELECT Id, UnixStartTimeMs,
       COALESCE(NULLIF(DisplayName, ''), Title) AS DisplayTitle,
       Server, Language, Patch, FightCount, DetailJson
FROM Sessions
ORDER BY UnixStartTimeMs DESC;" : @"
SELECT Id, UnixStartTimeMs, Title, Server, Language, Patch, FightCount, DetailJson
FROM Sessions
ORDER BY UnixStartTimeMs DESC;";
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        var list = new List<SessionSummary>();
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var id = Guid.Parse(r.GetString(0));
            var unix = r.GetInt64(1);
            var title = r.GetString(2);
            var server = r.IsDBNull(3) ? "" : r.GetString(3);
            var lang = r.IsDBNull(4) ? "" : r.GetString(4);
            var patch = r.IsDBNull(5) ? "" : r.GetString(5);
            var fightCount = r.GetInt32(6);

            int? trialInitKey = null;
            if (hasTrialInitKey)
            {
                if (!r.IsDBNull(7))
                    trialInitKey = r.GetInt32(7);
            }
            else
            {
                var json = r.IsDBNull(7) ? null : r.GetString(7);
                if (!string.IsNullOrWhiteSpace(json))
                {
                    try
                    {
                        var s = JsonSerializer.Deserialize<SessionDetail>(json, JsonOpts);
                        trialInitKey = s?.TrialInitKey;
                    }
                    catch
                    {
                        // ignore; keep null
                    }
                }
            }

            list.Add(new SessionSummary(id, title, unix, server, lang, patch, fightCount, trialInitKey));
        }

        return list;
    }


    public SessionDetail? GetSession(Guid sessionId)
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();

        var hasDisplayName = ColumnExists(conn, "Sessions", "DisplayName");

        var cmd = conn.CreateCommand();
        cmd.CommandText = hasDisplayName
            ? "SELECT DetailJson, DisplayName FROM Sessions WHERE Id=$id LIMIT 1;"
            : "SELECT DetailJson, NULL as DisplayName FROM Sessions WHERE Id=$id LIMIT 1;";
        cmd.Parameters.AddWithValue("$id", sessionId.ToString());

        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;

        var json = r.IsDBNull(0) ? null : r.GetString(0);
        if (string.IsNullOrWhiteSpace(json)) return null;

        var displayName = r.IsDBNull(1) ? null : r.GetString(1);
        var s = JsonSerializer.Deserialize<SessionDetail>(json, JsonOpts);
        if (s is null) return null;

        // Do not rewrite the stored JSON when the user renames a session; keep it as raw log data.
        if (!string.IsNullOrWhiteSpace(displayName))
            s = s with { Title = displayName };

        return s;
    }

    public void SetSessionDisplayName(Guid sessionId, string? displayName)
    {
        using var conn = OpenConnection(readOnly: false);
        conn.Open();

        EnsureSchema(conn);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE Sessions SET DisplayName=$dn WHERE Id=$id;";
        cmd.Parameters.AddWithValue("$id", sessionId.ToString());
        if (string.IsNullOrWhiteSpace(displayName))
            cmd.Parameters.AddWithValue("$dn", DBNull.Value);
        else
            cmd.Parameters.AddWithValue("$dn", displayName.Trim());
        cmd.ExecuteNonQuery();
    }

    public FightSummary? GetFight(Guid fightId)
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT SummaryJson FROM Fights WHERE Id=$id LIMIT 1;";
        cmd.Parameters.AddWithValue("$id", fightId.ToString());

        var json = cmd.ExecuteScalar() as string;
        if (string.IsNullOrWhiteSpace(json)) return null;
        return JsonSerializer.Deserialize<FightSummary>(json, JsonOpts);
    }

    public FightDetail? GetFightDetail(Guid fightId)
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT DetailJson FROM FightDetails WHERE FightId=$id LIMIT 1;";
        cmd.Parameters.AddWithValue("$id", fightId.ToString());

        var json = cmd.ExecuteScalar() as string;
        if (string.IsNullOrWhiteSpace(json)) return null;
        return JsonSerializer.Deserialize<FightDetail>(json, JsonOpts);
    }

    public IReadOnlyList<FightSeriesPoint> GetSeries(Guid fightId)
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT SeriesJson FROM FightSeries WHERE FightId=$id LIMIT 1;";
        cmd.Parameters.AddWithValue("$id", fightId.ToString());

        var json = cmd.ExecuteScalar() as string;
        if (string.IsNullOrWhiteSpace(json)) return Array.Empty<FightSeriesPoint>();

        IReadOnlyList<FightSeriesPoint>? series = JsonSerializer.Deserialize<List<FightSeriesPoint>>(json, JsonOpts);
        return series ?? Array.Empty<FightSeriesPoint>();
    }

    public FightRangeStats? GetRange(Guid fightId, long fromRelMs, long toRelMs)
    {
        if (toRelMs <= fromRelMs) return null;
        var series = GetSeries(fightId);
        if (series.Count == 0) return null;

        int fromSec = (int)Math.Floor(fromRelMs / 1000.0);
        int toSecExclusive = (int)Math.Ceiling(toRelMs / 1000.0);

        long dmg = 0;
        long heal = 0;

        foreach (var p in series)
        {
            if (p.Second < fromSec || p.Second >= toSecExclusive) continue;
            dmg += p.Damage;
            heal += p.Heal;
        }

        double durSec = (toRelMs - fromRelMs) / 1000.0;
        if (durSec <= 0) durSec = 0.001;

        return new FightRangeStats(
            FromRelMs: fromRelMs,
            ToRelMs: toRelMs,
            TotalDamage: dmg,
            TotalHeal: heal,
            Dps: dmg / durSec,
            Hps: heal / durSec
        );
    }

    // --------------------
    // Write API
    // --------------------

    public void UpsertSession(SessionDetail session)
    {
        var fightCount = 0;
        foreach (var z in session.Zones)
            fightCount += z.Fights.Count;

        var json = JsonSerializer.Serialize(session, JsonOpts);

        var conn = _bulkConn;
        if (conn is null)
        {
            using var c = OpenConnection();
            c.Open();
            EnsureSchema(c);
            using var tx = c.BeginTransaction();
            UpsertSessionCore(c, tx, session, fightCount, json);
            tx.Commit();
            return;
        }

        UpsertSessionCore(conn, _bulkTx, session, fightCount, json);
    }

    public void UpsertFight(FightSummary fight, IReadOnlyList<FightSeriesPoint> series, FightDetail detail)
    {
        var sumJson = JsonSerializer.Serialize(fight, JsonOpts);
        var detJson = JsonSerializer.Serialize(detail, JsonOpts);
        var serJson = JsonSerializer.Serialize(series, JsonOpts);

        var conn = _bulkConn;
        if (conn is null)
        {
            using var c = OpenConnection();
            c.Open();
            EnsureSchema(c);
            using var tx = c.BeginTransaction();
            UpsertFightCore(c, tx, fight, sumJson, detJson, serJson);
            tx.Commit();
            return;
        }

        UpsertFightCore(conn, _bulkTx, fight, sumJson, detJson, serJson);
    }

    // --------------------
    // Helpers / schema
    // --------------------

    internal IEnumerable<Guid> EnumerateFightIds()
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Id FROM Fights;";
        using var r = cmd.ExecuteReader();
        while (r.Read())
            yield return Guid.Parse(r.GetString(0));
    }

    internal (string? sourceFileName, DateTimeOffset? importedAtUtc) ReadMeta()
    {
        using var conn = OpenConnection(readOnly: true);
        conn.Open();
        string? source = GetMeta(conn, "SourceFileName");
        var importedRaw = GetMeta(conn, "ImportedAtUtc");
        DateTimeOffset? imported = null;
        if (!string.IsNullOrWhiteSpace(importedRaw) && DateTimeOffset.TryParse(importedRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
            imported = dto;
        return (source, imported);
    }

    private static SqliteConnection CreateConnection(string path, bool readOnly)
    {
        // Cache=Shared helps when multiple connections exist.
        var csb = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Cache = SqliteCacheMode.Shared,
            Mode = readOnly ? SqliteOpenMode.ReadOnly : SqliteOpenMode.ReadWriteCreate,
            // We frequently rename/delete/export per-log database files. Connection pooling can keep
            // file handles open for a short time, which breaks File.Move/Delete on Windows.
            Pooling = false,
            // Give SQLite some time to resolve transient locks (WAL checkpoints, etc.).
            DefaultTimeout = 30,
        };
        return new SqliteConnection(csb.ToString());
    }

    private SqliteConnection OpenConnection(bool readOnly = false)
        => CreateConnection(_dbPath, readOnly);

    private static void EnsureSchema(SqliteConnection conn)
    {
        // Ensure the connection is open before executing DDL.
        // Constructors call this with a freshly created connection.
        if (conn.State != System.Data.ConnectionState.Open)
            conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS LogMeta(
  Key TEXT PRIMARY KEY,
  Value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Sessions(
  Id TEXT PRIMARY KEY,
  UnixStartTimeMs INTEGER NOT NULL,
  Title TEXT NOT NULL,
  DisplayName TEXT,
  Server TEXT,
  Language TEXT,
  Patch TEXT,
  FightCount INTEGER NOT NULL,
  TrialInitKey INTEGER,
  DetailJson TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Fights(
  Id TEXT PRIMARY KEY,
  SessionId TEXT NOT NULL,
  ZoneSegmentId TEXT NOT NULL,
  StartRelMs INTEGER NOT NULL,
  EndRelMs INTEGER NOT NULL,
  Title TEXT NOT NULL,
  ZoneName TEXT NOT NULL,
  Difficulty TEXT,
  MapName TEXT,
  MapKey TEXT,
  IsHardMode INTEGER NOT NULL,
  SummaryJson TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS IX_Fights_SessionId ON Fights(SessionId);
CREATE INDEX IF NOT EXISTS IX_Fights_ZoneSegmentId ON Fights(ZoneSegmentId);

CREATE TABLE IF NOT EXISTS FightDetails(
  FightId TEXT PRIMARY KEY,
  DetailJson TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS FightSeries(
  FightId TEXT PRIMARY KEY,
  SeriesJson TEXT NOT NULL
);
";
        cmd.ExecuteNonQuery();

        // Older DBs (pre "display name") won't have the column. Add it if missing.
        if (!ColumnExists(conn, "Sessions", "DisplayName"))
        {
            using var alter = conn.CreateCommand();
            alter.CommandText = "ALTER TABLE Sessions ADD COLUMN DisplayName TEXT";
            try { alter.ExecuteNonQuery(); }
            catch { /* ignore */ }
        }

        if (!ColumnExists(conn, "Sessions", "TrialInitKey"))
        {
            using var alter = conn.CreateCommand();
            alter.CommandText = "ALTER TABLE Sessions ADD COLUMN TrialInitKey INTEGER";
            try { alter.ExecuteNonQuery(); }
            catch { /* ignore */ }
        }
    }

    private static bool ColumnExists(SqliteConnection conn, string table, string column)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"PRAGMA table_info({table});";
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            var name = r.GetString(1);
            if (string.Equals(name, column, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private static void UpsertSessionCore(SqliteConnection conn, SqliteTransaction? tx, SessionDetail session, int fightCount, string json)
    {
        using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = @"
INSERT INTO Sessions(Id, UnixStartTimeMs, Title, Server, Language, Patch, FightCount, TrialInitKey, DetailJson)
VALUES($id, $unix, $title, $server, $lang, $patch, $fightCount, $trialKey, $json)
ON CONFLICT(Id) DO UPDATE SET
  UnixStartTimeMs=excluded.UnixStartTimeMs,
  Title=excluded.Title,
  Server=excluded.Server,
  Language=excluded.Language,
  Patch=excluded.Patch,
  FightCount=excluded.FightCount,
  TrialInitKey=excluded.TrialInitKey,
  DetailJson=excluded.DetailJson;
";
        cmd.Parameters.AddWithValue("$id", session.Id.ToString());
        cmd.Parameters.AddWithValue("$unix", session.UnixStartTimeMs);
        cmd.Parameters.AddWithValue("$title", session.Title);
        cmd.Parameters.AddWithValue("$server", session.Server);
        cmd.Parameters.AddWithValue("$lang", session.Language);
        cmd.Parameters.AddWithValue("$patch", session.Patch);
        cmd.Parameters.AddWithValue("$fightCount", fightCount);
        if (session.TrialInitKey is null) cmd.Parameters.AddWithValue("$trialKey", DBNull.Value);
        else cmd.Parameters.AddWithValue("$trialKey", session.TrialInitKey.Value);
        cmd.Parameters.AddWithValue("$json", json);
        cmd.ExecuteNonQuery();
    }

    private static void UpsertFightCore(SqliteConnection conn, SqliteTransaction? tx, FightSummary fight, string sumJson, string detJson, string serJson)
    {
        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO Fights(Id, SessionId, ZoneSegmentId, StartRelMs, EndRelMs, Title, ZoneName, Difficulty, MapName, MapKey, IsHardMode, SummaryJson)
VALUES($id, $sid, $zid, $s, $e, $title, $zone, $diff, $mapName, $mapKey, $hm, $json)
ON CONFLICT(Id) DO UPDATE SET
  SessionId=excluded.SessionId,
  ZoneSegmentId=excluded.ZoneSegmentId,
  StartRelMs=excluded.StartRelMs,
  EndRelMs=excluded.EndRelMs,
  Title=excluded.Title,
  ZoneName=excluded.ZoneName,
  Difficulty=excluded.Difficulty,
  MapName=excluded.MapName,
  MapKey=excluded.MapKey,
  IsHardMode=excluded.IsHardMode,
  SummaryJson=excluded.SummaryJson;
";
            cmd.Parameters.AddWithValue("$id", fight.Id.ToString());
            cmd.Parameters.AddWithValue("$sid", fight.SessionId.ToString());
            cmd.Parameters.AddWithValue("$zid", fight.ZoneSegmentId.ToString());
            cmd.Parameters.AddWithValue("$s", fight.StartRelMs);
            cmd.Parameters.AddWithValue("$e", fight.EndRelMs);
            cmd.Parameters.AddWithValue("$title", fight.Title);
            cmd.Parameters.AddWithValue("$zone", fight.ZoneName);
            cmd.Parameters.AddWithValue("$diff", fight.Difficulty);
            cmd.Parameters.AddWithValue("$mapName", (object?)fight.MapName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$mapKey", (object?)fight.MapKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("$hm", fight.IsHardMode ? 1 : 0);
            cmd.Parameters.AddWithValue("$json", sumJson);
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO FightDetails(FightId, DetailJson)
VALUES($id, $json)
ON CONFLICT(FightId) DO UPDATE SET DetailJson=excluded.DetailJson;";
            cmd.Parameters.AddWithValue("$id", fight.Id.ToString());
            cmd.Parameters.AddWithValue("$json", detJson);
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO FightSeries(FightId, SeriesJson)
VALUES($id, $json)
ON CONFLICT(FightId) DO UPDATE SET SeriesJson=excluded.SeriesJson;";
            cmd.Parameters.AddWithValue("$id", fight.Id.ToString());
            cmd.Parameters.AddWithValue("$json", serJson);
            cmd.ExecuteNonQuery();
        }
    }

    private static async Task ExecAsync(SqliteConnection conn, string sql, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static string? GetMeta(SqliteConnection conn, string key)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Value FROM LogMeta WHERE Key=$k LIMIT 1;";
        cmd.Parameters.AddWithValue("$k", key);
        return cmd.ExecuteScalar() as string;
    }

    private static async Task SetMetaAsync(SqliteConnection conn, string key, string value, CancellationToken ct)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO LogMeta(Key, Value)
VALUES($k, $v)
ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;";
        cmd.Parameters.AddWithValue("$k", key);
        cmd.Parameters.AddWithValue("$v", value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
