namespace EsoLogViewer.Core.Models;

public enum ResourceKind
{
    Health,
    Magicka,
    Stamina,
    Ultimate,
    Unknown
}

/// <summary>
/// Downsampled per-second resource snapshot for a unit.
/// Values are current + max for each pool.
/// </summary>
public sealed record ResourcePoint(
    int Second,
    int Health,
    int HealthMax,
    int Magicka,
    int MagickaMax,
    int Stamina,
    int StaminaMax,
    int Ultimate,
    int UltimateMax);

/// <summary>
/// A resource gain/drain event (best-effort) parsed from COMBAT_EVENT POWER_* results.
/// Amount is positive for gains, negative for drains.
/// Receiver is the unit whose resource changed (target if present, otherwise source).
/// </summary>
public sealed record ResourceEvent(
    long RelMs,
    ResourceKind Kind,
    int ReceiverUnitId,
    int SourceUnitId,
    int AbilityId,
    long Amount,
    string Result);
