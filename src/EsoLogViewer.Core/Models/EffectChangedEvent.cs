namespace EsoLogViewer.Core.Models;

public sealed record EffectChangedEvent(
    long RelMs,
    string ChangeType,
    int EffectSlot,
    long EffectInstanceId,
    int AbilityId,
    int TargetUnitId,

    int? HealthCur, int? HealthMax,
    int? MagickaCur, int? MagickaMax,
    int? StaminaCur, int? StaminaMax,
    int? UltimateCur, int? UltimateMax,

    double? X, double? Y, double? Z,

    IReadOnlyList<string> Fields
);
