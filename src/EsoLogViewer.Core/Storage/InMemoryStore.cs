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
    FightRangeStats? GetRange(Guid fightId, long fromRelMs, long toRelMs);

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

    public IReadOnlyList<FightSeriesPoint> GetSeries(Guid fightId)
    {
        lock (_gate)
        {
            return _series.TryGetValue(fightId, out var s) ? s : Array.Empty<FightSeriesPoint>();
        }
    }

    public FightRangeStats? GetRange(Guid fightId, long fromRelMs, long toRelMs)
    {
        if (toRelMs <= fromRelMs) return null;

        List<FightSeriesPoint> series;
        lock (_gate)
        {
            if (!_series.TryGetValue(fightId, out series!))
                return null;

            // work on a copy
            series = new List<FightSeriesPoint>(series);
        }

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
