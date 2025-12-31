namespace EsoLogViewer.Core.Storage;

public sealed record MetaDbStats(
    int AbilityCount,
    int ItemCount,
    int SetCount,
    DateTimeOffset? LastImportUtc,
    string DbPath
);

/// <summary>
/// User-provided settings for resolving and displaying ESO UI icons.
/// Icon paths stored in the BSC saved variables typically look like "/esoui/art/icons/... .dds".
/// </summary>
public sealed record MetaDbUserSettings(
    /// <summary>
    /// Path to the folder that contains an <c>esoui</c> directory with <c>.dds</c> icons.
    /// Typically you point this to the <c>esoui</c> folder itself (e.g. <c>D:\...\esoui</c>).
    /// </summary>
    string? IconSourcePath,
    /// <summary>
    /// Root folder from which the app serves converted icons (PNG/WebP).
    /// Requests like <c>/usericons/esoui/art/icons/ability_*.png</c> are resolved under this root.
    /// </summary>
    string? IconRootPath,
    /// <summary>
    /// File extension used for converted icons (usually <c>png</c>).
    /// </summary>
    string IconFileExtension,

    /// <summary>
    /// If enabled, uploaded raw files (logs / saved variables) are kept on disk under the app's
    /// upload folder. This is mainly useful for debugging.
    /// </summary>
    bool KeepRawUploads = false,

    /// <summary>
    /// If enabled, buffs/debuffs that have the same display name but different ability IDs
    /// are grouped into a single row (ESO Logs-style).
    /// </summary>
    bool MergeEffectsByName = true
);

/// <summary>
/// User-defined annotations for an ability ID.
/// Used to group passives (e.g. "Medium Armor") and add personal notes.
/// </summary>
public sealed record AbilityUserMeta(
    int AbilityId,
    string? PassiveCategory,
    string? Note
);

/// <summary>
/// User-defined display names for trial keys (e.g. 19 => 'Cloudrest').
/// </summary>
public sealed record TrialKeyName(
    int TrialKey,
    string Name
);

public interface IMetaDb
{
    Task<MetaDbStats> GetStatsAsync(CancellationToken ct = default);
    Task<MetaDbUserSettings> GetUserSettingsAsync(CancellationToken ct = default);
    Task SetUserSettingsAsync(MetaDbUserSettings settings, CancellationToken ct = default);

    Task<IReadOnlyDictionary<int, AbilityUserMeta>> GetAbilityUserMetaAsync(IEnumerable<int> abilityIds, CancellationToken ct = default);
    Task<IReadOnlyList<AbilityUserMeta>> GetAllAbilityUserMetaAsync(CancellationToken ct = default);
    Task UpsertAbilityUserMetaAsync(AbilityUserMeta meta, CancellationToken ct = default);
    Task DeleteAbilityUserMetaAsync(int abilityId, CancellationToken ct = default);

    Task<IReadOnlyList<TrialKeyName>> GetAllTrialKeyNamesAsync(CancellationToken ct = default);
    Task UpsertTrialKeyNameAsync(TrialKeyName name, CancellationToken ct = default);
    Task DeleteTrialKeyNameAsync(int trialKey, CancellationToken ct = default);

    Task<IReadOnlyList<MetaAbility>> SearchAbilitiesAsync(string? query, bool passivesOnly = false, int limit = 50, CancellationToken ct = default);
    Task<IconCacheBuildResult> BuildIconCacheAsync(string sourcePath, bool overwrite = false, CancellationToken ct = default);
    Task ImportFromBscSavedVarsAsync(string filePath, CancellationToken ct = default);
    Task<IReadOnlyDictionary<int, MetaAbility>> GetAbilitiesAsync(IEnumerable<int> abilityIds, CancellationToken ct = default);
    Task<IReadOnlyDictionary<int, MetaItem>> GetItemsAsync(IEnumerable<int> itemIds, CancellationToken ct = default);
    Task<IReadOnlyDictionary<int, MetaSet>> GetSetsAsync(IEnumerable<int> setIds, CancellationToken ct = default);
    Task ResetAsync(CancellationToken ct = default);
}

public sealed record IconCacheBuildResult(
    string CacheRoot,
    int TotalDdsFiles,
    int Converted,
    int Skipped,
    int Failed
);

public sealed record MetaAbility(
    int AbilityId,
    string? Name,
    string? Texture,

    // Optional extended fields (filled if present in meta db).
    string? SkillLineName = null,
    int? SkillRankIndex = null,
    int? SkillLineRank = null
);

public sealed record MetaItem(
    int ItemId,
    string? Name,
    int? SetId,
    string? SetName,
    int ArmorType,
    int EquipType,
    int WeaponType,
    string? Icon
);

public sealed record MetaSet(
    int SetId,
    string? Name
);
