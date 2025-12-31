using EsoLogViewer.Core.Services;
using Microsoft.Data.Sqlite;
using System.Globalization;

namespace EsoLogViewer.Core.Storage;

/// <summary>
/// Stores addon metadata (abilities/items/sets) in a local SQLite database.
/// This is separate from the log session store, which is currently in-memory.
/// </summary>
public sealed class SqliteMetaDb : IMetaDb
{
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private bool _initialized;

    public string DbPath { get; }

    public SqliteMetaDb()
    {
        var root = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "EsoLogViewer");
        Directory.CreateDirectory(root);
        DbPath = Path.Combine(root, "meta.db");
    }

    public async Task<MetaDbStats> GetStatsAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        await using var conn = Open();
        await conn.OpenAsync(ct);

        var abilityCount = await ScalarIntAsync(conn, "SELECT COUNT(1) FROM Abilities", ct);
        var itemCount = await ScalarIntAsync(conn, "SELECT COUNT(1) FROM Items", ct);
        var setCount = await ScalarIntAsync(conn, "SELECT COUNT(1) FROM Sets", ct);

        DateTimeOffset? lastImport = null;
        var last = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='LastImportUtc'", ct);
        if (!string.IsNullOrWhiteSpace(last) && DateTimeOffset.TryParse(last, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            lastImport = dto;

        return new MetaDbStats(abilityCount, itemCount, setCount, lastImport, DbPath);
    }

    public async Task<MetaDbUserSettings> GetUserSettingsAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        var iconSource = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='IconSourcePath'", ct);
        var iconRoot = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='IconRootPath'", ct);
        var ext = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='IconFileExtension'", ct);
        var keep = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='KeepRawUploads'", ct);
        var merge = await ScalarStringAsync(conn, "SELECT Value FROM MetaInfo WHERE Key='MergeEffectsByName'", ct);

        // Default to PNG. Browsers/WebView don't render DDS.
        ext = string.IsNullOrWhiteSpace(ext) ? "png" : ext.Trim().TrimStart('.');

        var keepRawUploads = keep is not null && (keep == "1" || keep.Equals("true", StringComparison.OrdinalIgnoreCase));

        var mergeEffectsByName = merge is null || string.IsNullOrWhiteSpace(merge)
            ? true
            : (merge == "1" || merge.Equals("true", StringComparison.OrdinalIgnoreCase));

        return new MetaDbUserSettings(
            IconSourcePath: string.IsNullOrWhiteSpace(iconSource) ? null : iconSource,
            IconRootPath: string.IsNullOrWhiteSpace(iconRoot) ? null : iconRoot,
            IconFileExtension: ext,
            KeepRawUploads: keepRawUploads,
            MergeEffectsByName: mergeEffectsByName
        );
    }

    public async Task SetUserSettingsAsync(MetaDbUserSettings settings, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        var iconSource = string.IsNullOrWhiteSpace(settings.IconSourcePath) ? "" : settings.IconSourcePath.Trim();
        var iconRoot = string.IsNullOrWhiteSpace(settings.IconRootPath) ? "" : settings.IconRootPath.Trim();
        var ext = string.IsNullOrWhiteSpace(settings.IconFileExtension) ? "png" : settings.IconFileExtension.Trim().TrimStart('.');
        var keep = settings.KeepRawUploads ? "1" : "0";
        var merge = settings.MergeEffectsByName ? "1" : "0";

        await ExecAsync(conn, "INSERT INTO MetaInfo(Key, Value) VALUES('IconSourcePath', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct, ("$v", iconSource));

        await ExecAsync(conn, "INSERT INTO MetaInfo(Key, Value) VALUES('IconRootPath', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct, ("$v", iconRoot));

        await ExecAsync(conn, "INSERT INTO MetaInfo(Key, Value) VALUES('IconFileExtension', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct, ("$v", ext));

        await ExecAsync(conn, "INSERT INTO MetaInfo(Key, Value) VALUES('KeepRawUploads', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct, ("$v", keep));

        await ExecAsync(conn, "INSERT INTO MetaInfo(Key, Value) VALUES('MergeEffectsByName', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct, ("$v", merge));
    }

    public async Task<IconCacheBuildResult> BuildIconCacheAsync(string sourcePath, bool overwrite = false, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        // Convert into an app-local cache folder. The HTTP endpoint maps /usericons/* to this root.
        var cacheRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "EsoLogViewer", "usericons");
        Directory.CreateDirectory(cacheRoot);

        var result = await IconCacheBuilder.BuildAsync(sourcePath, cacheRoot, overwrite, ct);

        // Persist settings so the app can serve them.
        // Preserve other user settings (e.g. KeepRawUploads)
        var current = await GetUserSettingsAsync(ct);
        var settings = new MetaDbUserSettings(
            IconSourcePath: sourcePath,
            IconRootPath: cacheRoot,
            IconFileExtension: "png",
            KeepRawUploads: current.KeepRawUploads,
            MergeEffectsByName: current.MergeEffectsByName
        );
        await SetUserSettingsAsync(settings, ct);

        await using var conn = Open();
        await conn.OpenAsync(ct);
        await ExecAsync(conn,
            "INSERT INTO MetaInfo(Key, Value) VALUES('LastIconBuildUtc', $v) ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;",
            ct,
            ("$v", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)));

        return result;
    }

    public async Task ResetAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        await using var conn = Open();
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        await ExecAsync(conn, "DELETE FROM Abilities", ct);
        await ExecAsync(conn, "DELETE FROM Items", ct);
        await ExecAsync(conn, "DELETE FROM Sets", ct);
        // Keep AbilityUserMeta (personal notes/categories).
        // Keep user settings (e.g., icon root path), but clear import timestamp.
        await ExecAsync(conn, "DELETE FROM MetaInfo WHERE Key='LastImportUtc'", ct);
        await ExecAsync(conn, "DELETE FROM MetaInfo WHERE Key='LastIconBuildUtc'", ct);

        await tx.CommitAsync(ct);
    }

    public async Task<IReadOnlyDictionary<int, AbilityUserMeta>> GetAbilityUserMetaAsync(IEnumerable<int> abilityIds, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var ids = abilityIds?.Where(i => i > 0).Distinct().ToArray() ?? Array.Empty<int>();
        if (ids.Length == 0) return new Dictionary<int, AbilityUserMeta>();

        var result = new Dictionary<int, AbilityUserMeta>(ids.Length);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        foreach (var chunk in Chunk(ids, 500))
        {
            await using var cmd = conn.CreateCommand();
            var inList = AddInParameters(cmd, "$p", chunk);
            cmd.CommandText = $"SELECT AbilityId, PassiveCategory, Note FROM AbilityUserMeta WHERE AbilityId IN ({inList})";

            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var id = r.GetInt32(0);
                var cat = r.IsDBNull(1) ? null : r.GetString(1);
                var note = r.IsDBNull(2) ? null : r.GetString(2);
                result[id] = new AbilityUserMeta(id, cat, note);
            }
        }

        return result;
    }

    public async Task<IReadOnlyList<AbilityUserMeta>> GetAllAbilityUserMetaAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var list = new List<AbilityUserMeta>();
        await using var conn = Open();
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT AbilityId, PassiveCategory, Note FROM AbilityUserMeta ORDER BY AbilityId";
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var id = r.GetInt32(0);
            var cat = r.IsDBNull(1) ? null : r.GetString(1);
            var note = r.IsDBNull(2) ? null : r.GetString(2);
            list.Add(new AbilityUserMeta(id, cat, note));
        }

        return list;
    }

    public async Task UpsertAbilityUserMetaAsync(AbilityUserMeta meta, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        await ExecAsync(conn, @"
INSERT INTO AbilityUserMeta(AbilityId, PassiveCategory, Note)
VALUES($id, $cat, $note)
ON CONFLICT(AbilityId) DO UPDATE SET
  PassiveCategory=excluded.PassiveCategory,
  Note=excluded.Note;",
            ct,
            ("$id", meta.AbilityId),
            ("$cat", (object?)meta.PassiveCategory ?? DBNull.Value),
            ("$note", (object?)meta.Note ?? DBNull.Value));
    }

    public async Task DeleteAbilityUserMetaAsync(int abilityId, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        if (abilityId <= 0) return;
        await using var conn = Open();
        await conn.OpenAsync(ct);
        await ExecAsync(conn, "DELETE FROM AbilityUserMeta WHERE AbilityId=$id", ct, ("$id", abilityId));
    }


    public async Task<IReadOnlyList<TrialKeyName>> GetAllTrialKeyNamesAsync(CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var list = new List<TrialKeyName>();
        await using var conn = Open();
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT TrialKey, Name FROM TrialKeyNames ORDER BY TrialKey";
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var key = r.GetInt32(0);
            var name = r.IsDBNull(1) ? "" : r.GetString(1);
            if (!string.IsNullOrWhiteSpace(name))
                list.Add(new TrialKeyName(key, name));
        }

        return list;
    }

    public async Task UpsertTrialKeyNameAsync(TrialKeyName name, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        if (name.TrialKey <= 0) return;

        var n = (name.Name ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(n)) return;

        await using var conn = Open();
        await conn.OpenAsync(ct);

        await ExecAsync(conn, @"
INSERT INTO TrialKeyNames(TrialKey, Name)
VALUES($k, $n)
ON CONFLICT(TrialKey) DO UPDATE SET
  Name=excluded.Name;",
            ct,
            ("$k", name.TrialKey),
            ("$n", n));
    }

    public async Task DeleteTrialKeyNameAsync(int trialKey, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        if (trialKey <= 0) return;
        await using var conn = Open();
        await conn.OpenAsync(ct);
        await ExecAsync(conn, "DELETE FROM TrialKeyNames WHERE TrialKey=$k", ct, ("$k", trialKey));
    }

    public async Task<IReadOnlyList<MetaAbility>> SearchAbilitiesAsync(string? query, bool passivesOnly = false, int limit = 50, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        query = (query ?? string.Empty).Trim();
        if (limit <= 0) limit = 50;
        if (limit > 200) limit = 200;

        await using var conn = Open();
        await conn.OpenAsync(ct);
        await using var cmd = conn.CreateCommand();

        var where = "1=1";
        if (!string.IsNullOrWhiteSpace(query))
        {
            where += " AND (Name LIKE $q OR AbilityId = $id)";
            cmd.Parameters.AddWithValue("$q", "%" + query + "%");
            if (int.TryParse(query, out var aid)) cmd.Parameters.AddWithValue("$id", aid);
            else cmd.Parameters.AddWithValue("$id", -1);
        }
        if (passivesOnly)
            where += " AND IsPassive=1";

        cmd.CommandText = $"SELECT AbilityId, Name, Texture, SkillLineName, SkillRankIndex, SkillLineRank FROM Abilities WHERE {where} ORDER BY AbilityId LIMIT {limit};";

        var list = new List<MetaAbility>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var id = r.GetInt32(0);
            var name = r.IsDBNull(1) ? null : r.GetString(1);
            var tex = r.IsDBNull(2) ? null : r.GetString(2);
            var sln = r.IsDBNull(3) ? null : r.GetString(3);
            var sri = r.IsDBNull(4) ? (int?)null : r.GetInt32(4);
            var slr = r.IsDBNull(5) ? (int?)null : r.GetInt32(5);
            list.Add(new MetaAbility(id, name, tex, sln, sri, slr));
        }
        return list;
    }

    public async Task<IReadOnlyDictionary<int, MetaAbility>> GetAbilitiesAsync(IEnumerable<int> abilityIds, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var ids = abilityIds?.Where(i => i > 0).Distinct().ToArray() ?? Array.Empty<int>();
        if (ids.Length == 0) return new Dictionary<int, MetaAbility>();

        var result = new Dictionary<int, MetaAbility>(ids.Length);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        foreach (var chunk in Chunk(ids, 500))
        {
            await using var cmd = conn.CreateCommand();
            var inList = AddInParameters(cmd, "$p", chunk);
            cmd.CommandText = $"SELECT AbilityId, Name, Texture, SkillLineName, SkillRankIndex, SkillLineRank FROM Abilities WHERE AbilityId IN ({inList})";

            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var id = r.GetInt32(0);
                var name = r.IsDBNull(1) ? null : r.GetString(1);
                var tex = r.IsDBNull(2) ? null : r.GetString(2);
                var sln = r.IsDBNull(3) ? null : r.GetString(3);
                var sri = r.IsDBNull(4) ? (int?)null : r.GetInt32(4);
                var slr = r.IsDBNull(5) ? (int?)null : r.GetInt32(5);
                result[id] = new MetaAbility(id, name, tex, sln, sri, slr);
            }
        }

        return result;
    }

    public async Task<IReadOnlyDictionary<int, MetaItem>> GetItemsAsync(IEnumerable<int> itemIds, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var ids = itemIds?.Where(i => i > 0).Distinct().ToArray() ?? Array.Empty<int>();
        if (ids.Length == 0) return new Dictionary<int, MetaItem>();

        var result = new Dictionary<int, MetaItem>(ids.Length);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        foreach (var chunk in Chunk(ids, 500))
        {
            await using var cmd = conn.CreateCommand();
            var inList = AddInParameters(cmd, "$p", chunk);
            cmd.CommandText = $"SELECT ItemId, Name, SetId, SetName, ArmorType, EquipType, WeaponType, Icon FROM Items WHERE ItemId IN ({inList})";

            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var id = r.GetInt32(0);
                var name = r.IsDBNull(1) ? null : r.GetString(1);
                int? setId = r.IsDBNull(2) ? null : r.GetInt32(2);
                var setName = r.IsDBNull(3) ? null : r.GetString(3);
                var armorType = r.IsDBNull(4) ? 0 : r.GetInt32(4);
                var equipType = r.IsDBNull(5) ? 0 : r.GetInt32(5);
                var weaponType = r.IsDBNull(6) ? 0 : r.GetInt32(6);
                var icon = r.IsDBNull(7) ? null : r.GetString(7);

                result[id] = new MetaItem(id, name, setId, setName, armorType, equipType, weaponType, icon);
            }
        }

        return result;
    }

    public async Task<IReadOnlyDictionary<int, MetaSet>> GetSetsAsync(IEnumerable<int> setIds, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);
        var ids = setIds?.Where(i => i > 0).Distinct().ToArray() ?? Array.Empty<int>();
        if (ids.Length == 0) return new Dictionary<int, MetaSet>();

        var result = new Dictionary<int, MetaSet>(ids.Length);
        await using var conn = Open();
        await conn.OpenAsync(ct);

        foreach (var chunk in Chunk(ids, 500))
        {
            await using var cmd = conn.CreateCommand();
            var inList = AddInParameters(cmd, "$p", chunk);
            cmd.CommandText = $"SELECT SetId, Name FROM Sets WHERE SetId IN ({inList})";

            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var id = r.GetInt32(0);
                var name = r.IsDBNull(1) ? null : r.GetString(1);
                result[id] = new MetaSet(id, name);
            }
        }

        return result;
    }

    public async Task ImportFromBscSavedVarsAsync(string filePath, CancellationToken ct = default)
    {
        await EnsureInitializedAsync(ct);

        var text = await File.ReadAllTextAsync(filePath, ct);
        var reader = new BscSavedVarsReader(text);

        await using var conn = Open();
        await conn.OpenAsync(ct);
        // PRAGMA for speed during bulk import.
        // NOTE: SQLite forbids changing safety level (e.g. synchronous) while a transaction is active.
        // So we set PRAGMAs before beginning the transaction.
        await ExecAsync(conn, "PRAGMA synchronous=NORMAL", ct);
        await ExecAsync(conn, "PRAGMA journal_mode=WAL", ct);

        await using var tx = await conn.BeginTransactionAsync(ct);

        // Sets
        await using var setCmd = conn.CreateCommand();
        setCmd.CommandText = @"
INSERT INTO Sets(SetId, Name)
VALUES ($id, $name)
ON CONFLICT(SetId) DO UPDATE SET Name=excluded.Name;";
        var pSetId = setCmd.CreateParameter(); pSetId.ParameterName = "$id"; setCmd.Parameters.Add(pSetId);
        var pSetName = setCmd.CreateParameter(); pSetName.ParameterName = "$name"; setCmd.Parameters.Add(pSetName);

        foreach (var s in reader.ReadSets())
        {
            ct.ThrowIfCancellationRequested();
            pSetId.Value = s.SetId;
            pSetName.Value = (object?)s.Name ?? DBNull.Value;
            await setCmd.ExecuteNonQueryAsync(ct);
        }

        // Items
        await using var itemCmd = conn.CreateCommand();
        itemCmd.CommandText = @"
INSERT INTO Items(ItemId, Name, SetId, SetName, ArmorType, EquipType, WeaponType, Icon)
VALUES ($id, $name, $setId, $setName, $armor, $equip, $weapon, $icon)
ON CONFLICT(ItemId) DO UPDATE SET
  Name=excluded.Name,
  SetId=excluded.SetId,
  SetName=excluded.SetName,
  ArmorType=excluded.ArmorType,
  EquipType=excluded.EquipType,
  WeaponType=excluded.WeaponType,
  Icon=excluded.Icon;";
        var pItemId = itemCmd.CreateParameter(); pItemId.ParameterName = "$id"; itemCmd.Parameters.Add(pItemId);
        var pItemName = itemCmd.CreateParameter(); pItemName.ParameterName = "$name"; itemCmd.Parameters.Add(pItemName);
        var pItemSetId = itemCmd.CreateParameter(); pItemSetId.ParameterName = "$setId"; itemCmd.Parameters.Add(pItemSetId);
        var pItemSetName = itemCmd.CreateParameter(); pItemSetName.ParameterName = "$setName"; itemCmd.Parameters.Add(pItemSetName);
        var pArmor = itemCmd.CreateParameter(); pArmor.ParameterName = "$armor"; itemCmd.Parameters.Add(pArmor);
        var pEquip = itemCmd.CreateParameter(); pEquip.ParameterName = "$equip"; itemCmd.Parameters.Add(pEquip);
        var pWeapon = itemCmd.CreateParameter(); pWeapon.ParameterName = "$weapon"; itemCmd.Parameters.Add(pWeapon);
        var pIcon = itemCmd.CreateParameter(); pIcon.ParameterName = "$icon"; itemCmd.Parameters.Add(pIcon);

        foreach (var it in reader.ReadItems())
        {
            ct.ThrowIfCancellationRequested();
            pItemId.Value = it.ItemId;
            pItemName.Value = (object?)it.Name ?? DBNull.Value;
            pItemSetId.Value = it.SetId is null ? DBNull.Value : it.SetId.Value;
            pItemSetName.Value = (object?)it.SetName ?? DBNull.Value;
            pArmor.Value = it.ArmorType;
            pEquip.Value = it.EquipType;
            pWeapon.Value = it.WeaponType;
            pIcon.Value = (object?)it.Icon ?? DBNull.Value;
            await itemCmd.ExecuteNonQueryAsync(ct);
        }

        // Abilities
        await using var abCmd = conn.CreateCommand();
        abCmd.CommandText = @"
INSERT INTO Abilities(
  AbilityId, Name, Texture, Description, DescHeader, BuffType, IsUltimate, IsPassive, IsChanneled,
  CastTime, Cooldown, Duration, Radius, MinRange, MaxRange,
  SkillLineIndex, SkillLineId, SkillAbilityIndex, SkillRankIndex, SkillLineRank, SkillLineName,
  SkillLineDiscovered, SkillType, SkillMorphChoice, SkillKeySource
)
VALUES (
  $id, $name, $tex, $desc, $hdr, $buff, $ult, $pass, $chan,
  $cast, $cd, $dur, $rad, $min, $max,
  $sli, $slid, $sai, $sri, $slr, $sln,
  $sldisc, $stype, $smorph, $sks
)
ON CONFLICT(AbilityId) DO UPDATE SET
  Name=excluded.Name,
  Texture=excluded.Texture,
  Description=excluded.Description,
  DescHeader=excluded.DescHeader,
  BuffType=excluded.BuffType,
  IsUltimate=excluded.IsUltimate,
  IsPassive=excluded.IsPassive,
  IsChanneled=excluded.IsChanneled,
  CastTime=excluded.CastTime,
  Cooldown=excluded.Cooldown,
  Duration=excluded.Duration,
  Radius=excluded.Radius,
  MinRange=excluded.MinRange,
  MaxRange=excluded.MaxRange,
  SkillLineIndex=excluded.SkillLineIndex,
  SkillLineId=excluded.SkillLineId,
  SkillAbilityIndex=excluded.SkillAbilityIndex,
  SkillRankIndex=excluded.SkillRankIndex,
  SkillLineRank=excluded.SkillLineRank,
  SkillLineName=excluded.SkillLineName,
  SkillLineDiscovered=excluded.SkillLineDiscovered,
  SkillType=excluded.SkillType,
  SkillMorphChoice=excluded.SkillMorphChoice,
  SkillKeySource=excluded.SkillKeySource;";

        var pAbId = abCmd.CreateParameter(); pAbId.ParameterName = "$id"; abCmd.Parameters.Add(pAbId);
        var pAbName = abCmd.CreateParameter(); pAbName.ParameterName = "$name"; abCmd.Parameters.Add(pAbName);
        var pTex = abCmd.CreateParameter(); pTex.ParameterName = "$tex"; abCmd.Parameters.Add(pTex);
        var pDesc = abCmd.CreateParameter(); pDesc.ParameterName = "$desc"; abCmd.Parameters.Add(pDesc);
        var pHdr = abCmd.CreateParameter(); pHdr.ParameterName = "$hdr"; abCmd.Parameters.Add(pHdr);
        var pBuff = abCmd.CreateParameter(); pBuff.ParameterName = "$buff"; abCmd.Parameters.Add(pBuff);
        var pUlt = abCmd.CreateParameter(); pUlt.ParameterName = "$ult"; abCmd.Parameters.Add(pUlt);
        var pPass = abCmd.CreateParameter(); pPass.ParameterName = "$pass"; abCmd.Parameters.Add(pPass);
        var pChan = abCmd.CreateParameter(); pChan.ParameterName = "$chan"; abCmd.Parameters.Add(pChan);
        var pCast = abCmd.CreateParameter(); pCast.ParameterName = "$cast"; abCmd.Parameters.Add(pCast);
        var pCd = abCmd.CreateParameter(); pCd.ParameterName = "$cd"; abCmd.Parameters.Add(pCd);
        var pDur = abCmd.CreateParameter(); pDur.ParameterName = "$dur"; abCmd.Parameters.Add(pDur);
        var pRad = abCmd.CreateParameter(); pRad.ParameterName = "$rad"; abCmd.Parameters.Add(pRad);
        var pMin = abCmd.CreateParameter(); pMin.ParameterName = "$min"; abCmd.Parameters.Add(pMin);
        var pMax = abCmd.CreateParameter(); pMax.ParameterName = "$max"; abCmd.Parameters.Add(pMax);

        var pSli = abCmd.CreateParameter(); pSli.ParameterName = "$sli"; abCmd.Parameters.Add(pSli);
        var pSlid = abCmd.CreateParameter(); pSlid.ParameterName = "$slid"; abCmd.Parameters.Add(pSlid);
        var pSai = abCmd.CreateParameter(); pSai.ParameterName = "$sai"; abCmd.Parameters.Add(pSai);
        var pSri = abCmd.CreateParameter(); pSri.ParameterName = "$sri"; abCmd.Parameters.Add(pSri);
        var pSlr = abCmd.CreateParameter(); pSlr.ParameterName = "$slr"; abCmd.Parameters.Add(pSlr);
        var pSln = abCmd.CreateParameter(); pSln.ParameterName = "$sln"; abCmd.Parameters.Add(pSln);
        var pSlDisc = abCmd.CreateParameter(); pSlDisc.ParameterName = "$sldisc"; abCmd.Parameters.Add(pSlDisc);
        var pSType = abCmd.CreateParameter(); pSType.ParameterName = "$stype"; abCmd.Parameters.Add(pSType);
        var pSMorph = abCmd.CreateParameter(); pSMorph.ParameterName = "$smorph"; abCmd.Parameters.Add(pSMorph);
        var pSks = abCmd.CreateParameter(); pSks.ParameterName = "$sks"; abCmd.Parameters.Add(pSks);

        foreach (var a in reader.ReadAbilities())
        {
            ct.ThrowIfCancellationRequested();
            pAbId.Value = a.AbilityId;
            pAbName.Value = (object?)a.Name ?? DBNull.Value;
            pTex.Value = (object?)a.Texture ?? DBNull.Value;
            pDesc.Value = (object?)a.Description ?? DBNull.Value;
            pHdr.Value = (object?)a.DescHeader ?? DBNull.Value;
            pBuff.Value = a.BuffType;
            pUlt.Value = a.IsUltimate ? 1 : 0;
            pPass.Value = a.IsPassive ? 1 : 0;
            pChan.Value = a.IsChanneled ? 1 : 0;
            pCast.Value = a.CastTime;
            pCd.Value = a.Cooldown;
            pDur.Value = a.Duration;
            pRad.Value = a.Radius;
            pMin.Value = a.MinRange;
            pMax.Value = a.MaxRange;

            pSli.Value = a.SkillLineIndex;
            pSlid.Value = a.SkillLineId;
            pSai.Value = a.SkillAbilityIndex;
            pSri.Value = a.SkillRankIndex;
            pSlr.Value = a.SkillLineRank;
            pSln.Value = (object?)a.SkillLineName ?? DBNull.Value;
            pSlDisc.Value = a.SkillLineDiscovered ? 1 : 0;
            pSType.Value = a.SkillType;
            pSMorph.Value = a.SkillMorphChoice;
            pSks.Value = (object?)a.SkillKeySource ?? DBNull.Value;
            await abCmd.ExecuteNonQueryAsync(ct);
        }

        // Meta info
        await using var metaCmd = conn.CreateCommand();
        metaCmd.CommandText = @"
INSERT INTO MetaInfo(Key, Value)
VALUES ($k, $v)
ON CONFLICT(Key) DO UPDATE SET Value=excluded.Value;";
        var pk = metaCmd.CreateParameter(); pk.ParameterName = "$k"; metaCmd.Parameters.Add(pk);
        var pv = metaCmd.CreateParameter(); pv.ParameterName = "$v"; metaCmd.Parameters.Add(pv);

        pk.Value = "LastImportUtc";
        pv.Value = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        await metaCmd.ExecuteNonQueryAsync(ct);

        pk.Value = "Source";
        pv.Value = Path.GetFileName(filePath);
        await metaCmd.ExecuteNonQueryAsync(ct);

        await tx.CommitAsync(ct);
    }

    private SqliteConnection Open()
    {
        var csb = new SqliteConnectionStringBuilder
        {
            DataSource = DbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        };
        return new SqliteConnection(csb.ToString());
    }

    private async Task EnsureInitializedAsync(CancellationToken ct)
    {
        if (_initialized) return;
        await _initGate.WaitAsync(ct);
        try
        {
            if (_initialized) return;

            await using var conn = Open();
            await conn.OpenAsync(ct);

            await ExecAsync(conn, "PRAGMA foreign_keys=ON", ct);
            await ExecAsync(conn, "PRAGMA journal_mode=WAL", ct);

            await ExecAsync(conn, @"
CREATE TABLE IF NOT EXISTS MetaInfo(
  Key TEXT PRIMARY KEY,
  Value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Sets(
  SetId INTEGER PRIMARY KEY,
  Name TEXT
);

CREATE TABLE IF NOT EXISTS Items(
  ItemId INTEGER PRIMARY KEY,
  Name TEXT,
  SetId INTEGER,
  SetName TEXT,
  ArmorType INTEGER,
  EquipType INTEGER,
  WeaponType INTEGER,
  Icon TEXT
);

CREATE INDEX IF NOT EXISTS IX_Items_SetId ON Items(SetId);

CREATE TABLE IF NOT EXISTS Abilities(
  AbilityId INTEGER PRIMARY KEY,
  Name TEXT,
  Texture TEXT,
  Description TEXT,
  DescHeader TEXT,
  BuffType INTEGER,
  IsUltimate INTEGER,
  IsPassive INTEGER,
  IsChanneled INTEGER,
  CastTime REAL,
  Cooldown REAL,
  Duration REAL,
  Radius REAL,
  MinRange REAL,
  MaxRange REAL,

  -- Extended ability meta (v39+)
  SkillLineIndex INTEGER,
  SkillLineId INTEGER,
  SkillAbilityIndex INTEGER,
  SkillRankIndex INTEGER,
  SkillLineRank INTEGER,
  SkillLineName TEXT,
  SkillLineDiscovered INTEGER,
  SkillType INTEGER,
  SkillMorphChoice INTEGER,
  SkillKeySource TEXT
);

CREATE TABLE IF NOT EXISTS AbilityUserMeta(
  AbilityId INTEGER PRIMARY KEY,
  PassiveCategory TEXT,
  Note TEXT
);

CREATE TABLE IF NOT EXISTS TrialKeyNames(
  TrialKey INTEGER PRIMARY KEY,
  Name TEXT NOT NULL
);
", ct);

            // Lightweight migrations: ensure newly added columns exist.
            await EnsureAbilityColumnsAsync(conn, ct);

            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    private static IEnumerable<T[]> Chunk<T>(T[] arr, int size)
    {
        if (arr.Length <= size) { yield return arr; yield break; }
        for (int i = 0; i < arr.Length; i += size)
        {
            var n = Math.Min(size, arr.Length - i);
            var chunk = new T[n];
            Array.Copy(arr, i, chunk, 0, n);
            yield return chunk;
        }
    }

    private static async Task EnsureAbilityColumnsAsync(SqliteConnection conn, CancellationToken ct)
    {
        // Older meta dbs may miss newer columns. We keep this lightweight and non-destructive.
        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "PRAGMA table_info(Abilities);";
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                // PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
                var name = r.IsDBNull(1) ? null : r.GetString(1);
                if (!string.IsNullOrWhiteSpace(name)) existing.Add(name);
            }
        }

        async Task AddAsync(string name, string sqlType)
        {
            if (existing.Contains(name)) return;
            await ExecAsync(conn, $"ALTER TABLE Abilities ADD COLUMN {name} {sqlType};", ct);
            existing.Add(name);
        }

        // v39+ extended ability meta
        await AddAsync("SkillLineIndex", "INTEGER");
        await AddAsync("SkillLineId", "INTEGER");
        await AddAsync("SkillAbilityIndex", "INTEGER");
        await AddAsync("SkillRankIndex", "INTEGER");
        await AddAsync("SkillLineRank", "INTEGER");
        await AddAsync("SkillLineName", "TEXT");
        await AddAsync("SkillLineDiscovered", "INTEGER");
        await AddAsync("SkillType", "INTEGER");
        await AddAsync("SkillMorphChoice", "INTEGER");
        await AddAsync("SkillKeySource", "TEXT");
    }

    private static string AddInParameters(SqliteCommand cmd, string prefix, int[] ids)
    {
        // Returns e.g. "$p0,$p1,$p2" and adds parameters.
        var parts = new string[ids.Length];
        for (int i = 0; i < ids.Length; i++)
        {
            var name = $"{prefix}{i}";
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = ids[i];
            cmd.Parameters.Add(p);
            parts[i] = name;
        }
        return string.Join(",", parts);
    }

    private static async Task ExecAsync(SqliteConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(SqliteConnection conn, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = value;
            cmd.Parameters.Add(p);
        }
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> ScalarIntAsync(SqliteConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var o = await cmd.ExecuteScalarAsync(ct);
        return o is null || o == DBNull.Value ? 0 : Convert.ToInt32(o, CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarStringAsync(SqliteConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var o = await cmd.ExecuteScalarAsync(ct);
        return o is null || o == DBNull.Value ? null : Convert.ToString(o, CultureInfo.InvariantCulture);
    }
}
