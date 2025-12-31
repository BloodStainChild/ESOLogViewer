using EsoLogViewer.Core.Models;

namespace EsoLogViewer.Core.Storage;

public interface IStore
{
    IReadOnlyList<SessionSummary> GetSessions();
    SessionDetail? GetSession(Guid sessionId);

    /// <summary>
    /// Sets a user-facing display name for a session. Implementations may store it separately
    /// from the raw session title parsed from the log.
    /// </summary>
    void SetSessionDisplayName(Guid sessionId, string? displayName);

    FightSummary? GetFight(Guid fightId);
    FightDetail? GetFightDetail(Guid fightId);
    IReadOnlyList<FightSeriesPoint> GetSeries(Guid fightId);
    IReadOnlyCollection<int> GetCombatAbilityIds(
        Guid fightId,
        int? sourceUnitId = null,
        int? targetUnitId = null,
        bool heals = false);
    IReadOnlyList<FightSeriesPoint> GetCombatSeries(
        Guid fightId,
        int? sourceUnitId = null,
        int? targetUnitId = null,
        bool heals = false,
        IReadOnlyCollection<int>? abilityIds = null);
    FightRangeStats? GetRange(Guid fightId, long fromRelMs, long toRelMs);
    FightRangeStats? GetCombatRange(
        Guid fightId,
        long fromRelMs,
        long toRelMs,
        int? sourceUnitId = null,
        int? targetUnitId = null,
        bool heals = false,
        IReadOnlyCollection<int>? abilityIds = null);
    IReadOnlyList<CombatAggSummary> GetCombatAggs(
        Guid fightId,
        int? sourceUnitId = null,
        int? targetUnitId = null,
        bool heals = false,
        IReadOnlyCollection<int>? abilityIds = null);

    void UpsertSession(SessionDetail session);
    void UpsertFight(FightSummary fight, IReadOnlyList<FightSeriesPoint> series, FightDetail detail);
}

public sealed class InMemoryStore : IStore
{
    private readonly object _gate = new();

    private readonly Dictionary<Guid, SessionDetail> _sessions = new();
    private readonly Dictionary<Guid, FightSummary> _fights = new();
    private readonly Dictionary<Guid, FightDetail> _fightDetails = new();
    private readonly Dictionary<Guid, List<FightSeriesPoint>> _series = new();

    public IReadOnlyList<SessionSummary> GetSessions()
    {
        lock (_gate)
        {
            return _sessions.Values
                .Select(s =>
                {
                    int fightCount = 0;
                    foreach (var z in s.Zones)
                        fightCount += z.Fights.Count;

                    return new SessionSummary(
                        s.Id,
                        s.Title,
                        s.UnixStartTimeMs,
                        s.Server,
                        s.Language,
                        s.Patch,
                        fightCount,
                        s.TrialInitKey);
                })
                .OrderByDescending(s => s.UnixStartTimeMs)
                .ToList();
        }
    }

    public SessionDetail? GetSession(Guid sessionId)
    {
        lock (_gate)
        {
            return _sessions.TryGetValue(sessionId, out var s) ? s : null;
        }
    }

    public void SetSessionDisplayName(Guid sessionId, string? displayName)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(sessionId, out var s)) return;
            displayName = string.IsNullOrWhiteSpace(displayName) ? null : displayName.Trim();
            if (string.IsNullOrWhiteSpace(displayName)) return;
            _sessions[sessionId] = s with { Title = displayName };
        }
    }

    public FightSummary? GetFight(Guid fightId)
    {
        lock (_gate)
        {
            return _fights.TryGetValue(fightId, out var f) ? f : null;
        }
    }

    public FightDetail? GetFightDetail(Guid fightId)
    {
        lock (_gate)
        {
            return _fightDetails.TryGetValue(fightId, out var d) ? d : null;
        }
    }

    public IReadOnlyCollection<int> GetCombatAbilityIds(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false)
    {
        FightDetail? detail;
        lock (_gate)
        {
            _fightDetails.TryGetValue(fightId, out detail);
        }

        return CombatAggHelper.GetAbilityIds(detail, sourceUnitId, targetUnitId, heals);
    }

    public IReadOnlyList<CombatAggSummary> GetCombatAggs(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false, IReadOnlyCollection<int>? abilityIds = null)
    {
        FightDetail? detail;
        lock (_gate)
        {
            _fightDetails.TryGetValue(fightId, out detail);
        }

        return CombatAggHelper.ProjectAggregates(detail, sourceUnitId, targetUnitId, heals, abilityIds);
    }

    public IReadOnlyList<FightSeriesPoint> GetCombatSeries(Guid fightId, int? sourceUnitId = null, int? targetUnitId = null, bool heals = false, IReadOnlyCollection<int>? abilityIds = null)
    {
        FightDetail? detail;
        IReadOnlyList<FightSeriesPoint>? series;
        lock (_gate)
        {
            _fightDetails.TryGetValue(fightId, out detail);
            _series.TryGetValue(fightId, out var s);
            series = s;
        }

        return CombatAggHelper.ProjectSeries(detail, series, sourceUnitId, targetUnitId, heals, abilityIds);
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
        var filteredSeries = GetCombatSeries(fightId, sourceUnitId, targetUnitId, heals, abilityIds);
        return CombatAggHelper.ComputeRange(filteredSeries, fromRelMs, toRelMs);
    }

    public void UpsertSession(SessionDetail session)
    {
        lock (_gate)
        {
            _sessions[session.Id] = session;

            // keep fight index in sync (for quick Fight page routing)
            foreach (var z in session.Zones)
            {
                foreach (var f in z.Fights)
                {
                    _fights[f.Id] = f;
                    // details are inserted via UpsertFight
                }
            }
        }
    }

    public void UpsertFight(FightSummary fight, IReadOnlyList<FightSeriesPoint> series, FightDetail detail)
    {
        lock (_gate)
        {
            _fights[fight.Id] = fight;
            _series[fight.Id] = series.ToList();
            _fightDetails[fight.Id] = detail;
        }
    }
}
