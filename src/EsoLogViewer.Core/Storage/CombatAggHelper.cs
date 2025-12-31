using EsoLogViewer.Core.Models;

namespace EsoLogViewer.Core.Storage;

internal static class CombatAggHelper
{
    public static IReadOnlyList<FightSeriesPoint> ProjectSeries(
        FightDetail? detail,
        IReadOnlyList<FightSeriesPoint>? baseSeries,
        int? sourceUnitId,
        int? targetUnitId,
        bool heals)
    {
        var hasFilters = sourceUnitId is not null || targetUnitId is not null;
        var samples = detail?.CombatSamples;

        if (samples is null || samples.Count == 0)
        {
            if (baseSeries is null || baseSeries.Count == 0)
                return Array.Empty<FightSeriesPoint>();

            if (hasFilters)
                return Array.Empty<FightSeriesPoint>();

            if (heals)
                return baseSeries.Select(p => new FightSeriesPoint(p.Second, 0, p.Heal)).ToList();

            return baseSeries;
        }

        var buckets = new Dictionary<int, (long damage, long heal)>();

        foreach (var sample in samples)
        {
            if (sourceUnitId is not null && sample.SourceUnitId != sourceUnitId.Value)
                continue;
            if (targetUnitId is not null && sample.TargetUnitId != targetUnitId.Value)
                continue;

            var second = (int)(sample.RelMs / 1000);
            buckets.TryGetValue(second, out var cur);

            if (heals)
                cur.heal += sample.Heal;
            else
                cur.damage += sample.Damage;

            buckets[second] = cur;
        }

        return buckets
            .Select(kv => new FightSeriesPoint(
                Second: kv.Key,
                Damage: heals ? 0 : kv.Value.damage,
                Heal: heals ? kv.Value.heal : 0))
            .OrderBy(p => p.Second)
            .ToList();
    }

    public static IReadOnlyList<CombatAggSummary> ProjectAggregates(
        FightDetail? detail,
        int? sourceUnitId,
        int? targetUnitId,
        bool heals)
    {
        if (detail is null)
            return Array.Empty<CombatAggSummary>();

        var aggs = heals ? detail.HealAggs : detail.DamageAggs;
        if (aggs is null || aggs.Count == 0)
            return Array.Empty<CombatAggSummary>();

        var abilityTotals = new Dictionary<int, (long total, int hits, int crits, int activeSeconds, long overheal)>();

        foreach (var agg in aggs)
        {
            if (sourceUnitId is not null && agg.SourceUnitId != sourceUnitId.Value)
                continue;
            if (targetUnitId is not null && agg.TargetUnitId != targetUnitId.Value)
                continue;

            abilityTotals.TryGetValue(agg.AbilityId, out var cur);
            cur.total += agg.Total;
            cur.hits += agg.Hits;
            cur.crits += agg.Crits;
            cur.activeSeconds += agg.ActiveSeconds;
            cur.overheal += agg.Overheal;
            abilityTotals[agg.AbilityId] = cur;
        }

        var grandTotal = abilityTotals.Values.Sum(v => v.total);

        return abilityTotals
            .Select(kv =>
            {
                var total = kv.Value.total;
                var hits = kv.Value.hits;
                var activeSeconds = kv.Value.activeSeconds;

                double dps = activeSeconds > 0 ? total / (double)activeSeconds : total;
                double avg = hits > 0 ? total / (double)hits : 0;
                double critPct = hits > 0 ? kv.Value.crits / (double)hits : 0;
                double percent = grandTotal > 0 ? total / (double)grandTotal : 0;

                return new CombatAggSummary(
                    AbilityId: kv.Key,
                    Total: total,
                    Hits: hits,
                    Crits: kv.Value.crits,
                    ActiveSeconds: activeSeconds,
                    Overheal: kv.Value.overheal,
                    Dps: dps,
                    Average: avg,
                    CritPct: critPct,
                    Percent: percent);
            })
            .OrderByDescending(s => s.Total)
            .ToList();
    }

    public static FightRangeStats? ComputeRange(
        IReadOnlyList<FightSeriesPoint>? series,
        long fromRelMs,
        long toRelMs)
    {
        if (toRelMs <= fromRelMs || series is null)
            return null;

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
}
