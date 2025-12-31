using EsoLogViewer.Core.Models;
using EsoLogViewer.Core.Storage;

namespace EsoLogViewer.Core.Services;

/// <summary>
/// Imports a raw ESO encounter log into a dedicated SQLite database file.
/// </summary>
public sealed class LogDbImporter : ILogImporter
{
    private readonly MultiLogStore _multi;

    public LogDbImporter(MultiLogStore multi)
    {
        _multi = multi;
    }

    public async Task<IReadOnlyList<SessionSummary>> ImportAsync(string path, CancellationToken ct = default)
    {
        // Create one SQLite database per imported log so it can be shared/deleted.
        Directory.CreateDirectory(MultiLogStore.LogDbRoot);

        var safeName = Path.GetFileName(path);
        if (string.IsNullOrWhiteSpace(safeName)) safeName = "Encounter.log";
        safeName = string.Join("_", safeName.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));

        var baseName = safeName.EndsWith(".log", StringComparison.OrdinalIgnoreCase)
            ? safeName[..^4]
            : safeName;

        // Import into a temp db first, then rename to a nicer file name derived from the log content.
        var tmpDbPath = Path.Combine(MultiLogStore.LogDbRoot, $"import_{Guid.NewGuid():N}.log.db");
        IReadOnlyList<SessionSummary> imported;

        var store = new SqliteLogStore(tmpDbPath, ensureSchema: true);
        try
        {
            await store.BeginBulkImportAsync(sourceLogFileName: safeName, ct);
            var importer = new LogImporter(store);
            imported = await importer.ImportAsync(path, ct);
            await store.EndBulkImportAsync(ct);
        }
        catch
        {
            // Incomplete/broken dbs are useless. Best-effort cleanup.
            try { if (File.Exists(tmpDbPath)) File.Delete(tmpDbPath); } catch { /* ignore */ }
            throw;
        }
        finally
        {
            await store.DisposeAsync();
        }

        // Extra safety: ensure all SQLite pooled connections are closed before we rename the file.
        // (Even with Pooling=false this is harmless and protects against future code changes.)
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();

        // Choose a stable timestamp: earliest session start in the file.
        var minStart = imported.Count == 0 ? (long?)null : imported.Min(s => s.UnixStartTimeMs);
        var stamp = minStart is null
            ? DateTimeOffset.Now
            : DateTimeOffset.FromUnixTimeMilliseconds(minStart.Value).ToLocalTime();

        var niceFileName = $"{baseName}_{stamp:yyyy-MM-dd_HH-mm-ss}.log.db";
        niceFileName = string.Join("_", niceFileName.Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));
        if (string.IsNullOrWhiteSpace(niceFileName)) niceFileName = $"Encounter_{stamp:yyyy-MM-dd_HH-mm-ss}.log.db";

        var finalPath = Path.Combine(MultiLogStore.LogDbRoot, niceFileName);
        finalPath = MakeUnique(finalPath);

        File.Move(tmpDbPath, finalPath);

        // Make the new db visible in the UI.
        _multi.Refresh();
        return imported;
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
}
