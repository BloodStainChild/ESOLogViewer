namespace EsoLogViewer.Core.Models;

public sealed record SessionSummary(
    Guid Id,
    string Title,
    long UnixStartTimeMs,
    string Server,
    string Language,
    string Patch,
    int FightCount,
    int? TrialInitKey = null
);

/// <summary>
/// A single BEGIN_LOG..END_LOG block.
/// </summary>
public sealed record SessionDetail(
    Guid Id,
    string Title,
    long UnixStartTimeMs,
    string Server,
    string Language,
    string Patch,
    IReadOnlyDictionary<int, AbilityDefinition> Abilities,
    IReadOnlyDictionary<int, EffectDefinition> Effects,
    IReadOnlyList<PlayerInfoSnapshot> PlayerInfos,
    IReadOnlyList<UnitInfo> Units,
    IReadOnlyList<ZoneSegment> Zones,
    IReadOnlyList<TrialRun> Trials,
    IReadOnlyDictionary<string, int> UnhandledEventTypes,
    int? TrialInitKey = null
);

/// <summary>
/// A unit (player, monster, object, etc.) as announced by UNIT_* events.
/// Stored per session so later parsing stages can map "unit id" to names/accounts/etc.
/// </summary>
public sealed record UnitInfo(
    int UnitId,
    string UnitType,
    bool IsLocalPlayer,
    int? GroupIndex,
    int? MonsterId,
    bool IsBoss,
    int? ClassId,
    int? RaceId,
    string Name,
    string Account,
    ulong CharacterId,
    int? Level,
    int? ChampionPoints,
    string Disposition,
    bool IsGrouped,
    bool IsActive,
    long FirstSeenRelMs,
    long LastSeenRelMs
);

public sealed record AbilityDefinition(
    int AbilityId,
    string Name,
    string IconPath,
    bool IsHardModeMarker
);

/// <summary>
/// Snapshot of a player's build at a specific time (PLAYER_INFO).
/// </summary>
public sealed record PlayerInfoSnapshot(
    long RelMs,
    int UnitId,
    IReadOnlyList<int> PassiveAbilityIds,
    IReadOnlyList<int> PassiveRanks,
    IReadOnlyList<EquipmentItem> Equipment,
    IReadOnlyList<int> FrontBar,
    IReadOnlyList<int> BackBar
);

public sealed record EquipmentItem(
    string Slot,
    int ItemId,
    bool Flag,
    int Value,
    string Trait,
    string Quality,
    int SetId,
    string Enchant,
    bool EnchantFlag,
    int EnchantValue,
    string EnchantQuality
);

/// <summary>
/// A segment starting at a ZONE_CHANGED line (and ending at the next ZONE_CHANGED, or END_LOG).
/// Contains maps and fights that happened while being in that zone.
/// </summary>
public sealed record ZoneSegment(
    Guid Id,
    long StartRelMs,
    long? EndRelMs,
    int ZoneId,
    string ZoneName,
    string Difficulty,
    IReadOnlyList<MapChange> Maps,
    IReadOnlyList<FightSummary> Fights
);

public sealed record MapChange(
    long RelMs,
    int MapId,
    string MapName,
    string MapKey
);

public sealed record FightSummary(
    Guid Id,
    Guid SessionId,
    Guid ZoneSegmentId,
    long StartRelMs,
    long EndRelMs,
    string Title,
    string ZoneName,
    string Difficulty,
    string? MapName,
    string? MapKey,
    bool IsHardMode,
    IReadOnlyList<int> BossUnitIds,
    IReadOnlyList<string> BossNames
);

public sealed record FightSeriesPoint(
    int Second,
    long Damage,
    long Heal
);

public sealed record FightRangeStats(
    long FromRelMs,
    long ToRelMs,
    long TotalDamage,
    long TotalHeal,
    double Dps,
    double Hps
);
