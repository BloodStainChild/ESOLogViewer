using EsoLogViewer.Core.Storage;

namespace EsoLogViewer.Core.Services;

/// <summary>
/// Small cache + change notification for user-defined trial key display names.
/// Backed by the meta SQLite DB (meta.db).
/// </summary>
public interface ITrialKeyNameService
{
    event Action? Changed;

    Task<IReadOnlyDictionary<int, string>> GetAllAsync(CancellationToken ct = default);
    Task RefreshAsync(CancellationToken ct = default);

    Task UpsertAsync(int trialKey, string name, CancellationToken ct = default);
    Task DeleteAsync(int trialKey, CancellationToken ct = default);

    string? TryGetName(int trialKey);
}

public sealed class TrialKeyNameService : ITrialKeyNameService
{
    public event Action? Changed;

    private readonly IMetaDb _metaDb;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private readonly object _cacheGate = new();

    private Dictionary<int, string> _cache = new();
    private bool _loaded;

    public TrialKeyNameService(IMetaDb metaDb)
    {
        _metaDb = metaDb;
    }

    public string? TryGetName(int trialKey)
    {
        if (trialKey <= 0) return null;
        lock (_cacheGate)
        {
            return _cache.TryGetValue(trialKey, out var name) ? name : null;
        }
    }

    public async Task<IReadOnlyDictionary<int, string>> GetAllAsync(CancellationToken ct = default)
    {
        await EnsureLoadedAsync(ct);
        lock (_cacheGate)
        {
            return new Dictionary<int, string>(_cache);
        }
    }

    public async Task RefreshAsync(CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct);
        try
        {
            var list = await _metaDb.GetAllTrialKeyNamesAsync(ct);
            var dict = new Dictionary<int, string>();
            foreach (var x in list)
            {
                var name = (x.Name ?? string.Empty).Trim();
                if (x.TrialKey > 0 && !string.IsNullOrWhiteSpace(name))
                    dict[x.TrialKey] = name;
            }

            lock (_cacheGate)
            {
                _cache = dict;
                _loaded = true;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UpsertAsync(int trialKey, string name, CancellationToken ct = default)
    {
        name = (name ?? string.Empty).Trim();
        if (trialKey <= 0 || string.IsNullOrWhiteSpace(name)) return;

        await _metaDb.UpsertTrialKeyNameAsync(new TrialKeyName(trialKey, name), ct);
        await RefreshAsync(ct);
        Changed?.Invoke();
    }

    public async Task DeleteAsync(int trialKey, CancellationToken ct = default)
    {
        if (trialKey <= 0) return;

        await _metaDb.DeleteTrialKeyNameAsync(trialKey, ct);
        await RefreshAsync(ct);
        Changed?.Invoke();
    }

    private async Task EnsureLoadedAsync(CancellationToken ct)
    {
        if (_loaded) return;
        await RefreshAsync(ct);
    }
}
