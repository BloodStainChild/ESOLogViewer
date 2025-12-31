using System.Globalization;
using System.Text.RegularExpressions;
using EsoLogViewer.Core.Models;

namespace EsoLogViewer.Core.Storage;

public sealed record LogDatabaseInfo(
    string DbPath,
    string DisplayName,
    long FileSizeBytes,
    DateTimeOffset? ImportedAtUtc,
    int SessionCount,
    int FightCount
);

/// <summary>
/// Aggregates multiple per-log SQLite databases into a single read-only store.
/// Each imported encounter log is stored in its own SQLite file.
/// </summary>
public sealed class MultiLogStore : IStore
{
    public event Action? Changed;

    private readonly object _gate = new();
    private readonly Dictionary<Guid, string> _sessionToDb = new();
    private readonly Dictionary<Guid, string> _fightToDb = new();
    private List<SessionSummary> _sessions = new();
    private List<LogDatabaseInfo> _logDbs = new();

    public MultiLogStore()
    {
        Refresh();
    }

    public static string LogDbRoot
        => Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "EsoLogViewer", "logdbs");

    public IReadOnlyList<LogDatabaseInfo> GetLogDatabases()
    {
        lock (_gate)
        {
            return _logDbs.ToList();
        }
    }

    public void DeleteLogDatabase(string dbPath)
    {
        if (string.IsNullOrWhiteSpace(dbPath)) return;
        try
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
        }
        catch
        {
            // ignore
        }

        Refresh();
    }

    /// <summary>
    /// Renames existing per-log DB files to the new friendly naming scheme:
    /// &lt;LogBasename&gt;_YYYY-MM-dd_HH-mm-ss.log.db
    ///
    /// This is useful after upgrading from older versions that created temporary GUID-based filenames.
    /// </summary>
    public int RenameExistingLogDatabases()
    {
        Directory.CreateDirectory(LogDbRoot);

        // Already-friendly file name?
        var niceRx = new Regex(@"^.+_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.log\.db$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        var renamed = 0;
        foreach (var dbPath in Directory.EnumerateFiles(LogDbRoot, "*.log.db", SearchOption.TopDirectoryOnly))
        {
            var fileName = Path.GetFileName(dbPath);
            if (string.IsNullOrWhiteSpace(fileName))
                continue;

            if (niceRx.IsMatch(fileName))
                continue; // already OK

            try
            {
                var store = new SqliteLogStore(dbPath, ensureSchema: false);
                var sessions = store.GetSessions();
                var (source, importedAt) = store.ReadMeta();

                var baseName = string.IsNullOrWhiteSpace(source)
                    ? Path.GetFileName(fileName)
                    : Path.GetFileName(source);

                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Encounter.log";

                baseName = baseName.EndsWith(".log", StringComparison.OrdinalIgnoreCase)
                    ? baseName[..^4]
                    : baseName;

                baseName = string.Join("_", baseName.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Encounter";

                var minStart = sessions.Count == 0 ? (long?)null : sessions.Min(s => s.UnixStartTimeMs);
                var stamp = minStart is not null
                    ? DateTimeOffset.FromUnixTimeMilliseconds(minStart.Value).ToLocalTime()
                    : (importedAt?.ToLocalTime() ?? new DateTimeOffset(File.GetLastWriteTimeUtc(dbPath), TimeSpan.Zero).ToLocalTime());

                var newName = $"{baseName}_{stamp:yyyy-MM-dd_HH-mm-ss}.log.db";
                newName = string.Join("_", newName.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));
                if (string.IsNullOrWhiteSpace(newName))
                    newName = $"Encounter_{stamp:yyyy-MM-dd_HH-mm-ss}.log.db";

                var target = Path.Combine(LogDbRoot, newName);
                target = MakeUnique(target);

                // If locked, this may throw. We'll skip and keep going.
                if (!string.Equals(dbPath, target, StringComparison.OrdinalIgnoreCase))
                {
                    File.Move(dbPath, target);
                    renamed++;
                }
            }
            catch
            {
                // ignore and continue
            }
        }

        if (renamed > 0)
            Refresh();

        return renamed;
    }

    private static string MakeUnique(string path)
    {
        if (!File.Exists(path)) return path;

        var dir = Path.GetDirectoryName(path) ?? ".";
        var name = Path.GetFileNameWithoutExtension(path);
        var ext = Path.GetExtension(path);

        for (int i = 2; i < 10_000; i++)
        {
            var candidate = Path.Combine(dir, $"{name}_{i}{ext}");
            if (!File.Exists(candidate)) return candidate;
        }
        return Path.Combine(dir, $"{name}_{Guid.NewGuid():N}{ext}");
    }

    public void Refresh()
    {
        Directory.CreateDirectory(LogDbRoot);

        var dbFiles = Directory.EnumerateFiles(LogDbRoot, "*.log.db", SearchOption.TopDirectoryOnly)
            .OrderByDescending(File.GetLastWriteTimeUtc)
            .ToList();

        var sessionToDb = new Dictionary<Guid, string>();
        var fightToDb = new Dictionary<Guid, string>();
        var sessions = new List<SessionSummary>();
        var dbInfos = new List<LogDatabaseInfo>();

        foreach (var dbPath in dbFiles)
        {
            try
            {
                var store = new SqliteLogStore(dbPath, ensureSchema: false);

                var ss = store.GetSessions();
                foreach (var s in ss)
                    sessionToDb[s.Id] = dbPath;

                // Build fight index (used for /fight/{id} routing)
                foreach (var fid in store.EnumerateFightIds())
                    fightToDb[fid] = dbPath;

                sessions.AddRange(ss);

                var (source, importedAt) = store.ReadMeta();
                var fi = new FileInfo(dbPath);
                var fightCount = 0;
                foreach (var s in ss) fightCount += s.FightCount;
                dbInfos.Add(new LogDatabaseInfo(
                    DbPath: dbPath,
                    DisplayName: string.IsNullOrWhiteSpace(source) ? Path.GetFileName(dbPath) : source!,
                    FileSizeBytes: fi.Exists ? fi.Length : 0,
                    ImportedAtUtc: importedAt,
                    SessionCount: ss.Count,
                    FightCount: fightCount
                ));
            }
            catch
            {
                // ignore broken db
            }
        }

        sessions = sessions.OrderByDescending(s => s.UnixStartTimeMs).ToList();
        dbInfos = dbInfos.OrderByDescending(d => d.ImportedAtUtc ?? DateTimeOffset.MinValue).ToList();

        lock (_gate)
        {
            _sessionToDb.Clear();
            foreach (var kv in sessionToDb) _sessionToDb[kv.Key] = kv.Value;

            _fightToDb.Clear();
            foreach (var kv in fightToDb) _fightToDb[kv.Key] = kv.Value;

            _sessions = sessions;
            _logDbs = dbInfos;
        }

        Changed?.Invoke();
    }

    public IReadOnlyList<SessionSummary> GetSessions()
    {
        lock (_gate)
        {
            return _sessions.ToList();
        }
    }

    public SessionDetail? GetSession(Guid sessionId)
    {
        string? path;
        lock (_gate)
        {
            _sessionToDb.TryGetValue(sessionId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return null;

        try
        {
            return new SqliteLogStore(path, ensureSchema: false).GetSession(sessionId);
        }
        catch
        {
            return null;
        }
    }

    public void SetSessionDisplayName(Guid sessionId, string? displayName)
    {
        string? path;
        lock (_gate)
        {
            _sessionToDb.TryGetValue(sessionId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return;

        try
        {
            var store = new SqliteLogStore(path, ensureSchema: false);
            store.SetSessionDisplayName(sessionId, displayName);
        }
        catch
        {
            // ignore
        }

        Refresh();
    }

    public FightSummary? GetFight(Guid fightId)
    {
        string? path;
        lock (_gate)
        {
            _fightToDb.TryGetValue(fightId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return null;

        try
        {
            return new SqliteLogStore(path, ensureSchema: false).GetFight(fightId);
        }
        catch
        {
            return null;
        }
    }

    public FightDetail? GetFightDetail(Guid fightId)
    {
        string? path;
        lock (_gate)
        {
            _fightToDb.TryGetValue(fightId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return null;

        try
        {
            return new SqliteLogStore(path, ensureSchema: false).GetFightDetail(fightId);
        }
        catch
        {
            return null;
        }
    }

    public IReadOnlyCollection<int> GetCombatAbilityIds(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false)
    {
        string? path;
        lock (_gate)
        {
            _fightToDb.TryGetValue(fightId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return Array.Empty<int>();

        try
        {
            var store = new SqliteLogStore(path, ensureSchema: false);
            return store.GetCombatAbilityIds(fightId, sourceUnitId, targetUnitId, heals);
        }
        catch
        {
            return Array.Empty<int>();
        }
    }

    public IReadOnlyList<CombatAggSummary> GetCombatAggs(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false, IReadOnlyCollection<int>? abilityIds = null)
    {
        var detail = GetFightDetail(fightId);
        return CombatAggHelper.ProjectAggregates(detail, sourceUnitId, targetUnitId, heals, abilityIds);
    }

    public IReadOnlyList<FightSeriesPoint> GetCombatSeries(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false, IReadOnlyCollection<int>? abilityIds = null)
    {
        string? path;
        lock (_gate)
        {
            _fightToDb.TryGetValue(fightId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return Array.Empty<FightSeriesPoint>();

        try
        {
            var store = new SqliteLogStore(path, ensureSchema: false);
            return store.GetCombatSeries(fightId, sourceUnitId, targetUnitId, heals, abilityIds);
        }
        catch
        {
            return Array.Empty<FightSeriesPoint>();
        }
    }

    public IReadOnlyList<FightSeriesPoint> GetSeries(Guid fightId)
    {
        return GetCombatSeries(fightId);
    }

    public FightRangeStats? GetRange(Guid fightId, long fromRelMs, long toRelMs)
    {
        return GetCombatRange(fightId, fromRelMs, toRelMs);
    }

    public FightRangeStats? GetCombatRange(Guid fightId, long fromRelMs, long toRelMs, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false, IReadOnlyCollection<int>? abilityIds = null)
    {
        string? path;
        lock (_gate)
        {
            _fightToDb.TryGetValue(fightId, out path);
        }

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return null;

        try
        {
            var store = new SqliteLogStore(path, ensureSchema: false);
            return store.GetCombatRange(fightId, fromRelMs, toRelMs, sourceUnitId, targetUnitId, heals, abilityIds);
        }
        catch
        {
            return null;
        }
    }

    // MultiLogStore is read-only. Importing happens into a per-log SqliteLogStore.
    public void UpsertSession(SessionDetail session) => throw new NotSupportedException();
    public void UpsertFight(FightSummary fight, IReadOnlyList<FightSeriesPoint> series, FightDetail detail) => throw new NotSupportedException();

    public static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return bytes + " B";
        double b = bytes;
        string[] units = { "B", "KB", "MB", "GB" };
        int u = 0;
        while (b >= 1024 && u < units.Length - 1)
        {
            b /= 1024;
            u++;
        }
        return b.ToString("0.##", CultureInfo.InvariantCulture) + " " + units[u];
    }
}
