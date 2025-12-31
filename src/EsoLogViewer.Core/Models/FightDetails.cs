namespace EsoLogViewer.Core.Models;

/// <summary>
/// EFFECT_INFO metadata, emitted out-of-combat.
/// </summary>
public sealed record EffectDefinition(
    int AbilityId,
    string EffectKind,
    string DamageType,
    string DurationType,
    int? LinkedAbilityId
);

public sealed record EffectUptime(
    long TotalMs,
    int Applications
);

public sealed record CastEntry(
    long StartRelMs,
    long? EndRelMs,
    int CasterUnitId,
    int AbilityId,
    string Result,

    // NEW (optional extras; existing code compiles because defaults exist)
    long CastInstanceId = 0,

    int? HealthCur = null, int? HealthMax = null,
    int? MagickaCur = null, int? MagickaMax = null,
    int? StaminaCur = null, int? StaminaMax = null,
    int? UltimateCur = null, int? UltimateMax = null,

    double? X = null, double? Y = null, double? Z = null,

    IReadOnlyList<string>? BeginFields = null,
    IReadOnlyList<string>? EndFields = null
)
{
    public long DurationMs => EndRelMs is null ? 0 : Math.Max(0, EndRelMs.Value - StartRelMs);
}

public sealed record DeathEvent(
    long RelMs,
    int VictimUnitId,
    int KillerUnitId,
    int AbilityId,
    long Damage,
    string Result
);

public sealed record UnitCombatTotals(
    long DamageDone,
    long DamageTaken,
    long HealingDone,
    long HealingTaken,
    long ResourceGained,
    int Deaths,
    int Casts
);

/// <summary>
/// Minimal per-event combat sample used for filtered timelines.
/// We keep this intentionally small to avoid bloating the per-fight JSON.
/// </summary>
public sealed record CombatSample(
    long RelMs,
    int SourceUnitId,
    int TargetUnitId,
    int AbilityId,
    long Damage,
    long Heal,
    long Overheal,
    bool IsCrit,
    string Result
);

public sealed record FightDetail(
    Guid FightId,
    IReadOnlyList<int> FriendlyUnitIds,
    IReadOnlyList<int> EnemyUnitIds,
    IReadOnlyDictionary<int, UnitCombatTotals> TotalsByUnit,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> DamageDoneByUnitAbility,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> DamageTakenByUnitAbility,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> HealingDoneByUnitAbility,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> HealingTakenByUnitAbility,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> ResourceGainedByUnitAbility,
    IReadOnlyDictionary<int, IReadOnlyList<ResourcePoint>> ResourcesByUnit,
    IReadOnlyList<ResourceEvent> ResourceEvents,
    IReadOnlyDictionary<int, IReadOnlyDictionary<int, EffectUptime>> EffectUptimesByTarget,
    IReadOnlyList<CastEntry> Casts,
    IReadOnlyList<DeathEvent> Deaths,
    IReadOnlyDictionary<string, int> UnhandledEventTypes,
    IReadOnlyList<EffectChangedEvent> EffectChanges,
    IReadOnlyList<HealthRegenEvent> HealthRegens,

    // --- v34: ESO-Logs-like filtering / richer per-ability tables (optional, defaults keep old DBs readable)
    IReadOnlyList<CombatAgg>? DamageAggs = null,
    IReadOnlyList<CombatAgg>? HealAggs = null,

    // --- v36: optional per-event samples for filtered timelines
    IReadOnlyList<CombatSample>? CombatSamples = null
);


