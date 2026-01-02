namespace EsoLogViewer.Core.Storage;

/// <summary>
/// Lightweight DTO for summarized/aggregated combat values.
///
/// NOTE: Some helper utilities (e.g. CombatAggHelper) depend on this type.
/// Keep it backwards-compatible: properties are optional and can be left at defaults.
/// </summary>
public sealed class CombatAggSummary
{
    public int SourceUnitId { get; init; }
    public int TargetUnitId { get; init; }
    public int AbilityId { get; init; }

    /// <summary>Total damage/heal amount.</summary>
    public long Total { get; init; }

    /// <summary>Alias sometimes used by UI/helpers.</summary>
    public long Amount => Total;

    /// <summary>Total overheal (heals only; 0 for damage).</summary>
    public long Overheal { get; init; }

    public int Hits { get; init; }
    public int Crits { get; init; }
    public int ActiveSeconds { get; init; }

    /// <summary>Damage per second (or Heal per second depending on context).</summary>
    public double Dps { get; init; }

    /// <summary>Heal per second (optional; used by some aggregations).</summary>
    public double Hps { get; init; }

    /// <summary>Average amount per hit/tick (optional; depends on aggregation).</summary>
    public double Average { get; init; }

    /// <summary>Critical hit percentage (0..100).</summary>
    public double CritPct { get; init; }

    /// <summary>Percent contribution (0..100) within the current scope.</summary>
    public double Percent { get; init; }

    public CombatAggSummary() { }

    /// <summary>
    /// Constructor with parameter names matching the public property names.
    /// This enables convenient use with C# named arguments (e.g. AbilityId: ..., Dps: ...).
    ///
    /// Important: The first parameters are optional so callers can use a minimal named-arg set
    /// (e.g. AbilityId/Total/Hits/...) without having to pass Source/Target.
    /// </summary>
    public CombatAggSummary(
        int SourceUnitId = 0,
        int TargetUnitId = 0,
        int AbilityId = 0,
        long Total = 0,
        int Hits = 0,
        int Crits = 0,
        int ActiveSeconds = 0,
        long Overheal = 0,
        double Dps = 0,
        double Hps = 0,
        double Average = 0,
        double CritPct = 0,
        double Percent = 0)
    {
        this.SourceUnitId = SourceUnitId;
        this.TargetUnitId = TargetUnitId;
        this.AbilityId = AbilityId;
        this.Total = Total;
        this.Hits = Hits;
        this.Crits = Crits;
        this.ActiveSeconds = ActiveSeconds;
        this.Overheal = Overheal;
        this.Dps = Dps;
        this.Hps = Hps;
        this.Average = Average;
        this.CritPct = CritPct;
        this.Percent = Percent;
    }
}
