namespace EsoLogViewer.Core.Models;

/// <summary>
/// Aggregated combat stats for a (source, target, ability) triple.
/// Used to support ESO-Logs-like source/target filtering without storing every single combat event.
/// </summary>
/// <param name="SourceUnitId">Actor that caused the event (attacker/healer).</param>
/// <param name="TargetUnitId">Recipient of the event (victim/healed). Use 0 if no explicit target was logged.</param>
/// <param name="AbilityId">Ability id.</param>
/// <param name="Total">Total damage/heal amount.</param>
/// <param name="Hits">Number of logged hits/ticks.</param>
/// <param name="Crits">Number of logged critical hits/ticks.</param>
/// <param name="ActiveSeconds">Unique seconds with at least one hit/tick (best-effort).</param>
/// <param name="Overheal">Total overheal amount (heals only; 0 for damage).</param>
public sealed record CombatAgg(
    int SourceUnitId,
    int TargetUnitId,
    int AbilityId,
    long Total,
    int Hits,
    int Crits,
    int ActiveSeconds,
    long Overheal = 0
);
