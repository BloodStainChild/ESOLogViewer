using System.Globalization;
using EsoLogViewer.Core.Models;
using EsoLogViewer.Core.Parsing;
using EsoLogViewer.Core.Storage;

namespace EsoLogViewer.Core.Services;

public interface ILogImporter
{
    Task<IReadOnlyList<SessionSummary>> ImportAsync(string path, CancellationToken ct = default);
}

/// <summary>
/// Streaming importer that can handle very large ESO log files.
/// The importer splits BEGIN_LOG..END_LOG into sessions, and BEGIN_COMBAT..END_COMBAT into fights.
/// </summary>
public sealed class LogImporter : ILogImporter
{
    private readonly IStore _store;

    public LogImporter(IStore store)
    {
        _store = store;
    }

    public async Task<IReadOnlyList<SessionSummary>> ImportAsync(string path, CancellationToken ct = default)
    {
        // Collect imported session summaries without querying the DB while a bulk transaction is open.
        var importedSessions = new List<SessionSummary>();

        SessionBuilder? session = null;

        using var fs = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 1024 * 1024,
            FileOptions.SequentialScan);

        using var sr = new StreamReader(fs);

        string? line;
        while ((line = await sr.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            if (!TryParseHeader(line, out var relMs, out var type))
                continue;

            // Session boundaries
            if (type == "BEGIN_LOG")
            {
                // finalize any broken previous session
                if (session is not null)
                {
                    var summary = FinalizeSession(session);
                    importedSessions.Add(summary);
                }

                var f = CsvTokenizer.Tokenize(line);
                var unixStart = TryParseLong(Get(f, 2));
                var server = Get(f, 4);
                var language = Get(f, 5);
                var patch = Get(f, 6);

                session = new SessionBuilder(unixStart, server, language, patch);
                continue;
            }

            if (session is null)
                continue;

            if (type == "END_LOG")
            {
                session.MarkSeen(type);
                var summary = FinalizeSession(session);
                    importedSessions.Add(summary);
                session = null;
                continue;
            }

            // Normal parsing
            switch (type)
            {
                case "ZONE_CHANGED":    // Fertig
                    {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddZoneChange(relMs, TryParseInt(Get(f, 2)), Get(f, 3), Get(f, 4));
                    break;
                }
                case "MAP_CHANGED":     // Fertig
                    {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddMapChange(relMs, TryParseInt(Get(f, 2)), Get(f, 3), Get(f, 4));
                    break;
                }
                case "UNIT_ADDED":      // Fertig
                    {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddUnitAdded(relMs, f);
                    break;
                }
                case "UNIT_CHANGED":    // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddUnitChanged(relMs, f);
                    break;
                }
                case "UNIT_REMOVED":    // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddUnitRemoved(relMs, f);
                    break;
                }
                case "ABILITY_INFO":    // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddAbilityInfo(f);
                    break;
                }
                case "EFFECT_INFO":     // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddEffectInfo(f);
                    break;
                }
                case "PLAYER_INFO":     // Fertig
                {
                    var f = CsvTokenizer.TokenizeWithBrackets(line);
                    session.AddPlayerInfo(f);
                    break;
                }
                case "BEGIN_COMBAT":    // Fertig
                    session.BeginCombat(relMs);
                    break;
                case "END_COMBAT":      // Fertig
                    session.EndCombat(relMs);
                    break;
                case "COMBAT_EVENT":
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddCombatEvent(relMs, f);
                    break;
                }
                case "EFFECT_CHANGED":  // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddEffectChanged(relMs, f);
                    break;
                }
                case "BEGIN_CAST":      // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddBeginCast(relMs, f);
                    break;
                }
                case "END_CAST":        // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddEndCast(relMs, f);
                    break;
                }
                case "BEGIN_TRIAL":     // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.BeginTrial(relMs, f);
                    break;
                }
                case "END_TRIAL":       // Fertig
                {
                    var f = CsvTokenizer.Tokenize(line);
                    session.EndTrial(relMs, f);
                    break;
                }
                case "TRIAL_INIT":       // TrialKey merken (für Session-Gruppierung)
                case "TRAIL_INIT":
                {
                    // 5850585,TRIAL_INIT,19,F,F,0,0,F,0
                    var f = CsvTokenizer.Tokenize(line);
                    session.SetTrialInitKey(relMs, f);
                    break;
                }
                case "HEALTH_REGEN":     // Fertig
                    {
                    var f = CsvTokenizer.Tokenize(line);
                    session.AddHealthRegen(relMs, f);
                    break;
                }
                default:
                    session.MarkUnparsed(type);
                    break;
            }

            session.MarkSeen(type);
        }

        if (session is not null)
        {
            var summary = FinalizeSession(session);
                    importedSessions.Add(summary);
        }

        return importedSessions;
    }

    private SessionSummary FinalizeSession(SessionBuilder session)
    {
        var detail = session.BuildDetail();
        _store.UpsertSession(detail);

        foreach (var (fight, series, fightDetail) in session.BuildFights())
        {
            _store.UpsertFight(fight, series, fightDetail);
        }

        var fightCount = 0;
        foreach (var z in detail.Zones)
            fightCount += z.Fights.Count;

        return new SessionSummary(detail.Id, detail.Title, detail.UnixStartTimeMs, detail.Server, detail.Language, detail.Patch, fightCount, detail.TrialInitKey);

}

    private static bool TryParseHeader(string line, out long relMs, out string type)
    {
        relMs = 0;
        type = "";

        if (string.IsNullOrWhiteSpace(line))
            return false;

        // In case file starts with BOM
        line = line.TrimStart('\uFEFF');

        int c1 = line.IndexOf(',');
        if (c1 <= 0) return false;

        if (!long.TryParse(line.AsSpan(0, c1), NumberStyles.Integer, CultureInfo.InvariantCulture, out relMs))
            return false;

        int c2 = line.IndexOf(',', c1 + 1);

        // Handles "2541,BEGIN_COMBAT" (no second comma)
        if (c2 < 0)
        {
            type = line.Substring(c1 + 1).Trim();
            return type.Length > 0;
        }

        type = line.Substring(c1 + 1, c2 - (c1 + 1)).Trim();
        return type.Length > 0;
    }


    private static string Get(IReadOnlyList<string> f, int i) => i >= 0 && i < f.Count ? f[i] : "";

    private static long TryParseLong(string s)
        => long.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

    private static int TryParseInt(string s)
        => int.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : int.MinValue;

    private static int? TryParseNullableInt(string s)
        => int.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : null;

    private static ulong? TryParseNullableULong(string s)
        => ulong.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : null;

    private static bool TryParseBool(string s)
    {
        s = s.Trim();
        return s.Equals("T", StringComparison.OrdinalIgnoreCase) || s.Equals("TRUE", StringComparison.OrdinalIgnoreCase);
    }

    private sealed class SessionBuilder
    {
        public Guid SessionId { get; } = Guid.NewGuid();

        private readonly long _unixStartTimeMs;
        private readonly string _server;
        private readonly string _language;
        private readonly string _patch;

        private string _title;

        private readonly Dictionary<int, AbilityDefinition> _abilities = new();
        private readonly Dictionary<int, EffectDefinition> _effects = new();
        private readonly Dictionary<int, UnitInfo> _units = new();

        // Unit ids can be reused after UNIT_REMOVED. Keep a history of lifetimes so
        // we can resolve correct names/accounts/loadouts for older fights.
        private readonly List<UnitInfo> _unitHistory = new();
        private readonly Dictionary<int, int> _activeUnitHistoryIndex = new();
        private readonly List<PlayerInfoSnapshot> _playerInfos = new();
        private readonly List<ZoneSegment> _zones = new();
        private readonly Dictionary<string, int> _unhandled = new(StringComparer.OrdinalIgnoreCase);

        private int _currentZoneIndex = -1;
        private MapChange? _currentMap;

        private FightDraft? _fight;
        private readonly List<(FightSummary Summary, List<FightSeriesPoint> Series, FightDetail Detail)> _fights = new();

        // Hard mode markers: collected from ABILITY_INFO names.
        private readonly HashSet<int> _hardModeAbilityIds = new();

        private TrialDraft? _trial;
        private readonly List<TrialRun> _trials = new();

        // TRIAL_INIT: the active trial key for the whole session (used for grouping)
        private int? _trialInitKey;

        public SessionBuilder(long unixStartTimeMs, string server, string language, string patch)
        {
            _unixStartTimeMs = unixStartTimeMs;
            _server = server;
            _language = language;
            _patch = patch;

            _title = $"Session_{DateTimeOffset.FromUnixTimeMilliseconds(unixStartTimeMs):yyyyMMdd_HHmmss}";
        }

        public void MarkSeen(string type)
        {
            // no-op for now; kept for future metrics
        }

        public void MarkUnparsed(string type)
        {
            if (string.IsNullOrWhiteSpace(type)) return;
            _unhandled.TryGetValue(type, out var cur);
            _unhandled[type] = cur + 1;
            _fight?.MarkUnparsed(type);
        }

        public void AddZoneChange(long relMs, int zoneId, string zoneName, string difficulty)
        {
            // Zone segments live for the whole session; fights reference the active zone.
            var seg = new ZoneSegment(
                Id: Guid.NewGuid(),
                StartRelMs: relMs,
                EndRelMs: null,
                ZoneId: zoneId == int.MinValue ? 0 : zoneId,
                ZoneName: zoneName,
                Difficulty: difficulty,
                Maps: new List<MapChange>(),
                Fights: new List<FightSummary>());

            if (_currentZoneIndex >= 0)
            {
                var last = _zones[_currentZoneIndex];
                _zones[_currentZoneIndex] = last with { EndRelMs = relMs };
            }

            _zones.Add(seg);
            _currentZoneIndex = _zones.Count - 1;
        }

        public void AddMapChange(long relMs, int mapId, string mapName, string mapKey)
        {
            var map = new MapChange(relMs, mapId, mapName, mapKey);
            _currentMap = map;

            if (_zones.Count == 0)
            {
                // Create a pseudo-zone if we got a map before a zone.
                AddZoneChange(relMs, 0, mapName, "NONE");
            }

            if (_zones[^1].Maps is List<MapChange> maps)
            {
                maps.Add(map);
            }
        }

        public void AddAbilityInfo(IReadOnlyList<string> f)
        {
            // rel, ABILITY_INFO, id, name, icon, isPassive, isPlayer
            var abilityId = TryParseInt(Get(f, 2));
            if (abilityId == int.MinValue) return;

            var name = Get(f, 3);
            var icon = Get(f, 4);
            var isPassive = TryParseBool(Get(f, 5));
            var isPlayer = TryParseBool(Get(f, 6));

            bool isHmMarker = name.Contains("Hard Mode", StringComparison.OrdinalIgnoreCase)
                              || name.Contains("HM", StringComparison.OrdinalIgnoreCase) && name.Contains("Mode", StringComparison.OrdinalIgnoreCase);

            _abilities[abilityId] = new AbilityDefinition(abilityId, name, icon, isHmMarker);
            if (isHmMarker) _hardModeAbilityIds.Add(abilityId);
        }

        public void AddEffectInfo(IReadOnlyList<string> f)
        {
            // rel, EFFECT_INFO, abilityId, BUFF/DEBUFF, damageType, durationType, [linkedAbilityId]
            var abilityId = TryParseInt(Get(f, 2));
            if (abilityId == int.MinValue) return;

            var kind = Get(f, 3);
            var dmg = Get(f, 4);
            var dur = Get(f, 5);

            int? linkedAbilityId = null;
            if (f.Count > 6)
            {
                var link = TryParseInt(Get(f, 6));
                if (link != int.MinValue) linkedAbilityId = link;
            }

            _effects[abilityId] = new EffectDefinition(abilityId, kind, dmg, dur, linkedAbilityId);
        }

        public void AddUnitAdded(long relMs, IReadOnlyList<string> f)
        {
            // rel, UNIT_ADDED, unitId, unitType, isLocal, groupIndex, monsterId, isBoss, classId, raceId, name, account, characterId, level, cp, ..., disposition, isGrouped
            var unitId = TryParseInt(Get(f, 2));
            if (unitId == int.MinValue) return;

            var unitType = Get(f, 3);
            var isLocal = TryParseBool(Get(f, 4));
            var groupIndex = TryParseNullableInt(Get(f, 5));
            var monsterId = TryParseNullableInt(Get(f, 6));
            var isBoss = TryParseBool(Get(f, 7));
            var classId = TryParseNullableInt(Get(f, 8));
            var raceId = TryParseNullableInt(Get(f, 9));
            var name = Get(f, 10);
            var account = Get(f, 11);
            var characterId = TryParseNullableULong(Get(f, 12)) ?? 0UL;
            var level = TryParseNullableInt(Get(f, 13)) ?? 0;
            var championPoints = TryParseNullableInt(Get(f, 14)) ?? 0;
            var disposition = Get(f, 16);
            var isGrouped = TryParseBool(Get(f, 17));

            // Close a previous active instance if the id was reused.
            if (_activeUnitHistoryIndex.TryGetValue(unitId, out var idx) && idx >= 0 && idx < _unitHistory.Count)
            {
                var prev = _unitHistory[idx];
                if (prev.IsActive)
                    _unitHistory[idx] = prev with { IsActive = false, LastSeenRelMs = relMs };
            }

            var u = new UnitInfo(
                unitId,
                unitType,
                isLocal,
                groupIndex,
                monsterId,
                isBoss,
                classId,
                raceId,
                name,
                account,
                characterId,
                level,
                championPoints,
                disposition,
                isGrouped,
                IsActive: true,
                FirstSeenRelMs: relMs,
                LastSeenRelMs: relMs);

            // Keep the current/latest view for parsing.
            _units[unitId] = u;

            // Also store as a separate lifetime entry.
            _unitHistory.Add(u);
            _activeUnitHistoryIndex[unitId] = _unitHistory.Count - 1;
        }


        public void AddUnitChanged(long relMs, IReadOnlyList<string> f)
        {
            // rel, UNIT_CHANGED, unitId, classId, raceId, name, account, characterId, level, cp, ..., disposition, isGrouped
            var unitId = TryParseInt(Get(f, 2));
            if (unitId == int.MinValue) return;

            _units.TryGetValue(unitId, out var existing);

            var classId = TryParseNullableInt(Get(f, 3));
            var raceId = TryParseNullableInt(Get(f, 4));
            var name = Get(f, 5);
            var account = Get(f, 6);
            var characterId = TryParseNullableULong(Get(f, 7)) ?? 0UL;
            var level = TryParseNullableInt(Get(f, 8)) ?? 0;
            var championPoints = TryParseNullableInt(Get(f, 9)) ?? 0;
            var disposition = Get(f, 11);
            var isGrouped = TryParseBool(Get(f, 12));

            if (existing is null)
            {
                var created = new UnitInfo(
                    unitId,
                    "UNKNOWN",
                    false,
                    null,
                    null,
                    false,
                    classId,
                    raceId,
                    name,
                    account,
                    characterId,
                    level,
                    championPoints,
                    disposition,
                    isGrouped,
                    IsActive: true,
                    FirstSeenRelMs: relMs,
                    LastSeenRelMs: relMs);

                _units[unitId] = created;
                _unitHistory.Add(created);
                _activeUnitHistoryIndex[unitId] = _unitHistory.Count - 1;
                return;
            }

            var updated = existing with
            {
                Name = string.IsNullOrWhiteSpace(name) ? existing.Name : name,
                Account = string.IsNullOrWhiteSpace(account) ? existing.Account : account,
                CharacterId = characterId != 0 ? characterId : existing.CharacterId,
                Level = level != 0 ? level : existing.Level,
                ChampionPoints = championPoints != 0 ? championPoints : existing.ChampionPoints,
                ClassId = classId ?? existing.ClassId,
                RaceId = raceId ?? existing.RaceId,
                Disposition = string.IsNullOrWhiteSpace(disposition) ? existing.Disposition : disposition,
                IsGrouped = isGrouped,
                IsActive = true,
                LastSeenRelMs = relMs
            };

            _units[unitId] = updated;

            if (_activeUnitHistoryIndex.TryGetValue(unitId, out var idx) && idx >= 0 && idx < _unitHistory.Count)
            {
                var cur = _unitHistory[idx];
                // Preserve the instance's FirstSeenRelMs.
                _unitHistory[idx] = updated with { FirstSeenRelMs = cur.FirstSeenRelMs };
            }
            else
            {
                // No active instance tracked (shouldn't happen often). Start a new lifetime entry.
                _unitHistory.Add(updated);
                _activeUnitHistoryIndex[unitId] = _unitHistory.Count - 1;
            }
        }


        public void AddUnitRemoved(long relMs, IReadOnlyList<string> f)
        {
            var unitId = TryParseInt(Get(f, 2));
            if (unitId == int.MinValue) return;

            if (_units.TryGetValue(unitId, out var u))
                _units[unitId] = u with { IsActive = false, LastSeenRelMs = relMs };

            if (_activeUnitHistoryIndex.TryGetValue(unitId, out var idx) && idx >= 0 && idx < _unitHistory.Count)
            {
                var cur = _unitHistory[idx];
                _unitHistory[idx] = cur with { IsActive = false, LastSeenRelMs = relMs };
                _activeUnitHistoryIndex.Remove(unitId);
            }
        }


        public void AddPlayerInfo(IReadOnlyList<string> f)
        {
            // rel, PLAYER_INFO, unitId, [passives], [ranks], [[gear]], [front], [back]
            var relMs = TryParseLong(Get(f, 0));
            var unitId = TryParseInt(Get(f, 2));
            if (unitId == int.MinValue) return;

            var passives = ParseIntList(Get(f, 3));
            var ranks = ParseIntList(Get(f, 4));
            var gear = ParseGear(Get(f, 5));
            var front = ParseIntList(Get(f, 6));
            var back = ParseIntList(Get(f, 7));

            _playerInfos.Add(new PlayerInfoSnapshot(relMs, unitId, passives, ranks, gear, front, back));
        }

        public void BeginCombat(long relMs)
        {
            if (_fight is not null)
                return; // nested - ignore

            if (_zones.Count == 0)
                AddZoneChange(relMs, 0, "Unknown", "NONE"); // zoneId=0 fallback

            _fight = new FightDraft(SessionId, relMs, _zones[^1], _currentMap);
        }

        public void EndCombat(long relMs)
        {
            if (_fight is null)
                return;

            _fight.EndRelMs = relMs;

            // Close open effects and casts.
            _fight.CloseOpen(relMs);

            // Boss list
            var bossUnits = _fight.UnitIdsSeen
                .Select(id => _units.TryGetValue(id, out var u) ? u : null)
                // Some encounters have units flagged as boss that are FRIENDLY/NPC_ALLY/etc.
                // Only treat HOSTILE boss units as fight bosses.
                .Where(u => u is not null
                            && u.IsBoss
                            && u.Disposition.Contains("HOSTILE", StringComparison.OrdinalIgnoreCase))
                .Cast<UnitInfo>()
                .ToList();

            var bossNames = bossUnits.Select(b => b.Name).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

            var title = bossNames.Count > 0
                ? string.Join(" + ", bossNames)
                : $"Fight {_fights.Count + 1}";

            var diff = _fight.Zone.Difficulty;

            // Build fight summary
            var summary = new FightSummary(
                Id: _fight.FightId,
                SessionId: SessionId,
                ZoneSegmentId: _fight.Zone.Id,
                StartRelMs: _fight.StartRelMs,
                EndRelMs: relMs,
                Title: title,
                ZoneName: _fight.Zone.ZoneName,
                Difficulty: diff,
                MapName: _fight.Map?.MapName,
                MapKey: _fight.Map?.MapKey,
                IsHardMode: _fight.IsHardMode,
                BossUnitIds: bossUnits.Select(b => b.UnitId).ToList(),
                BossNames: bossNames
            );

            // Attach to the zone segment so Sessions can show their fights
            var zidx = _zones.FindIndex(z => z.Id == _fight.Zone.Id);
            if (zidx >= 0 && _zones[zidx].Fights is List<FightSummary> fights)
                fights.Add(summary);

            // Series (timeline)
            var series = _fight.BuildSeries();

            // Fight detail
            var detail = _fight.BuildDetail(_units, _effects);

            _fights.Add((summary, series, detail));
            _fight = null;
        }

        public void AddCombatEvent(long relMs, IReadOnlyList<string> f)
        {
            _fight?.AddCombatEvent(relMs, f);
        }

        public void AddEffectChanged(long relMs, IReadOnlyList<string> f)
        {
            _fight?.AddEffectChanged(relMs, f, _hardModeAbilityIds);
        }

        public void AddBeginCast(long relMs, IReadOnlyList<string> f)
        {
            _fight?.AddBeginCast(relMs, f);
        }

        public void AddEndCast(long relMs, IReadOnlyList<string> f)
        {
            _fight?.AddEndCast(relMs, f);
        }


        public void SetTrialInitKey(long relMs, IReadOnlyList<string> f)
        {
            // 5850585,TRIAL_INIT,19,F,F,0,0,F,0
            var trialKey = TryParseInt(Get(f, 2));
            if (trialKey == int.MinValue) return;

            _trialInitKey = trialKey;
        }

        public void BeginTrial(long relMs, IReadOnlyList<string> f)
        {
            // 5917133,BEGIN_TRIAL,19,1763411611125
            var trialKey = TryParseInt(Get(f, 2));
            if (trialKey == int.MinValue) return;

            var unix = TryParseLong(Get(f, 3));
            if (unix <= 0)
                unix = _unixStartTimeMs + relMs;

            _trial = new TrialDraft(trialKey, relMs, unix, f.ToArray());
        }

        public void EndTrial(long relMs, IReadOnlyList<string> f)
        {
            // 7788836,END_TRIAL,19,1871687,T,286463,36000
            var trialKey = TryParseInt(Get(f, 2));
            if (trialKey == int.MinValue) return;

            var durationMs = TryParseLong(Get(f, 3));   // Trial Duration (ms)
            var success = TryParseBool(Get(f, 4));      // T/F
            var finalScore = TryParseLong(Get(f, 5));   // Final Score
            var vitality = TryParseInt(Get(f, 6));      // Vitality (z.B. 36000)

            // Fallbacks, falls BEGIN_TRIAL fehlt oder dur nicht parsebar ist
            var draft = _trial;
            if (draft is null || draft.TrialKey != trialKey)
            {
                var fallbackStartUnix = _unixStartTimeMs + relMs; // best-effort
                draft = new TrialDraft(trialKey, relMs, fallbackStartUnix, Array.Empty<string>());
            }

            // Falls duration nicht da ist, nimm rel-diff
            if (durationMs <= 0)
                durationMs = Math.Max(0, relMs - draft.StartRelMs);

            var startUnix = draft.StartUnixTimeMs;
            var endUnix = startUnix + durationMs; // besser als _unixStartTimeMs + relMs, weil END_TRIAL Dauer "offiziell" ist

            _trials.Add(new TrialRun(
                TrialKey: trialKey,
                StartRelMs: draft.StartRelMs,
                EndRelMs: relMs,
                StartUnixTimeMs: startUnix,
                EndUnixTimeMs: endUnix,
                DurationMs: durationMs,
                Success: success,
                FinalScore: finalScore < 0 ? 0 : finalScore,
                Vitality: vitality == int.MinValue ? 0 : vitality,
                BeginFields: draft.BeginFields,
                EndFields: f.ToArray()
            ));

            _trial = null;
        }

        public SessionDetail BuildDetail()
        {
            var fightsCount = _fights.Count + (_fight is null ? 0 : 1);
            var title = _title;

            return new SessionDetail(
                Id: SessionId,
                Title: title,
                UnixStartTimeMs: _unixStartTimeMs,
                Server: _server,
                Language: _language,
                Patch: _patch,
                Abilities: new Dictionary<int, AbilityDefinition>(_abilities),
                Effects: new Dictionary<int, EffectDefinition>(_effects),
                PlayerInfos: _playerInfos.ToList(),
                Units: _unitHistory.OrderBy(u => u.UnitId).ThenBy(u => u.FirstSeenRelMs).ToList(),
                Zones: _zones.ToList(),
                UnhandledEventTypes: new Dictionary<string, int>(_unhandled),
                Trials: _trials.ToList(),
                TrialInitKey: _trialInitKey
            );
        }
        public void AddHealthRegen(long relMs, IReadOnlyList<string> f)
        {
            _fight?.AddHealthRegen(relMs, f);
        }

        public IEnumerable<(FightSummary fight, IReadOnlyList<FightSeriesPoint> series, FightDetail detail)> BuildFights()
            => _fights.Select(f => (f.Summary, (IReadOnlyList<FightSeriesPoint>)f.Series, f.Detail));

        private static List<int> ParseIntList(string raw)
        {
            raw = raw.Trim();
            if (raw.StartsWith("[") && raw.EndsWith("]"))
                raw = raw[1..^1];

            var result = new List<int>();
            if (string.IsNullOrWhiteSpace(raw)) return result;

            foreach (var part in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (int.TryParse(part, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
                    result.Add(v);
            }

            return result;
        }

        private static List<EquipmentItem> ParseGear(string raw)
        {
            // raw is like [[NECK,194512,...],[CHEST,...]]
            raw = raw.Trim();
            if (!raw.StartsWith("[[") || !raw.EndsWith("]]"))
                return new List<EquipmentItem>();

            // strip outer [ [ and ] ]
            var inner = raw[2..^2];
            if (string.IsNullOrWhiteSpace(inner)) return new List<EquipmentItem>();

            // split top-level "],["
            var parts = inner.Split("],[", StringSplitOptions.RemoveEmptyEntries);
            var list = new List<EquipmentItem>(parts.Length);

            foreach (var p in parts)
            {
                var s = p.Trim('[', ']');
                var cols = s.Split(',', StringSplitOptions.TrimEntries);
                // PLAYER_INFO gear rows have 11 fields today (keep it flexible and default missing values).
                string slot = cols.ElementAtOrDefault(0) ?? "";
                int itemId = TryParseInt(cols.ElementAtOrDefault(1) ?? "");
                bool flag = TryParseBool(cols.ElementAtOrDefault(2) ?? "");
                int value = TryParseInt(cols.ElementAtOrDefault(3) ?? "");
                string trait = cols.ElementAtOrDefault(4) ?? "";
                string quality = cols.ElementAtOrDefault(5) ?? "";
                int setId = TryParseInt(cols.ElementAtOrDefault(6) ?? "");
                string enchant = cols.ElementAtOrDefault(7) ?? "";
                bool enchantFlag = TryParseBool(cols.ElementAtOrDefault(8) ?? "");
                int enchantValue = TryParseInt(cols.ElementAtOrDefault(9) ?? "");
                string enchantQuality = cols.ElementAtOrDefault(10) ?? "";

                if (itemId == int.MinValue) itemId = 0;
                if (value == int.MinValue) value = 0;
                if (setId == int.MinValue) setId = 0;
                if (enchantValue == int.MinValue) enchantValue = 0;

                list.Add(new EquipmentItem(slot, itemId, flag, value, trait, quality, setId, enchant, enchantFlag, enchantValue, enchantQuality));
            }

            return list;
        }

        private static string Get(IReadOnlyList<string> f, int i) => i >= 0 && i < f.Count ? f[i] : "";

        private sealed record TrialDraft(
            int TrialKey,
            long StartRelMs,
            long StartUnixTimeMs,
            IReadOnlyList<string> BeginFields
        );
    }

    private sealed class FightDraft
    {
        public Guid FightId { get; } = Guid.NewGuid();
        public Guid SessionId { get; }
        public long StartRelMs { get; }
        public long EndRelMs { get; set; }
        public ZoneSegment Zone { get; }
        public MapChange? Map { get; }

        public bool IsHardMode { get; private set; }

        public HashSet<int> UnitIdsSeen { get; } = new();

        private readonly Dictionary<int, long> _dmgPerSecond = new();
        private readonly Dictionary<int, long> _healPerSecond = new();

        private readonly Dictionary<int, Dictionary<int, long>> _dmgDoneByUnitAbility = new();
        private readonly Dictionary<int, Dictionary<int, long>> _dmgTakenByUnitAbility = new();
        private readonly Dictionary<int, Dictionary<int, long>> _healDoneByUnitAbility = new();
        private readonly Dictionary<int, Dictionary<int, long>> _healTakenByUnitAbility = new();
        private readonly Dictionary<int, Dictionary<int, long>> _resourceByUnitAbility = new();

        // Per-second (downsampled) resource snapshots for any unit seen in the fight.
        private readonly Dictionary<int, Dictionary<int, ResourcePointMutable>> _resourcesByUnitSecond = new();

        // POWER_* resource change events (best-effort).
        private readonly List<ResourceEvent> _resourceEvents = new();

        private readonly Dictionary<int, UnitTotalsMutable> _totals = new();

        private readonly List<DeathEvent> _deaths = new();
        private readonly List<CastEntry> _casts = new();

        private readonly Dictionary<EffectKey, long> _openEffects = new();
        private readonly Dictionary<int, Dictionary<int, EffectUptimeMutable>> _uptimes = new();

        private readonly Dictionary<CastKey, OpenCast> _openCasts = new();

        private readonly Dictionary<string, int> _unhandled = new(StringComparer.OrdinalIgnoreCase);

        private readonly List<EffectChangedEvent> _effectChanges = new();

        private readonly List<HealthRegenEvent> _healthRegens = new();

        // v34: richer aggregations that support source+target filtering (ESO Logs style)
        private readonly Dictionary<AggKey, AggMutable> _damageAgg = new();
        private readonly Dictionary<AggKey, AggMutable> _healAgg = new();

        // v36: minimal combat samples used for filtered timelines in the UI.
        private readonly List<CombatSample> _combatSamples = new();

        public FightDraft(Guid sessionId, long startRelMs, ZoneSegment zone, MapChange? map)
        {
            SessionId = sessionId;
            StartRelMs = startRelMs;
            Zone = zone;
            Map = map;
        }

        public void MarkUnparsed(string type)
        {
            _unhandled.TryGetValue(type, out var cur);
            _unhandled[type] = cur + 1;
        }

        public void AddCombatEvent(long relMs, IReadOnlyList<string> f)
        {
            // Base header (stable part)
            var result = Get(f, 2);
            var dmgType = Get(f, 3);
            var powerType = TryParseInt(Get(f, 4));
            var dmg = TryParseLong(Get(f, 5));
            var heal = TryParseLong(Get(f, 6));

            var sourceInstanceId = TryParseLong(Get(f, 7));
            var abilityId = TryParseInt(Get(f, 8));
            var srcUnitId = TryParseInt(Get(f, 9));

            // Track seen units
            if (srcUnitId != int.MinValue) UnitIdsSeen.Add(srcUnitId);

            int sec = (int)((relMs - StartRelMs) / 1000);
            if (sec < 0) sec = 0;

            // Parse the variable "unit blocks" (pools + unknown + xyz) starting after srcUnitId
            int i = 10;

            Pools? srcPools = null;
            WorldPos? srcPos = null;

            if (srcUnitId != int.MinValue && TryReadUnitBlock(f, ref i, out var sp, out var spos))
            {
                srcPools = sp;
                srcPos = spos;

                // Keep the per-second snapshot for resources tab (Health/Magicka/Stamina/Ultimate)
                AddResourceSample(srcUnitId, sec, sp);
            }

            int? tgtUnitId = null;
            Pools? tgtPools = null;
            WorldPos? tgtPos = null;

            // After source position, we either have "*" (no target) OR targetUnitId then target block
            if (i < f.Count)
            {
                var next = Get(f, i).Trim();
                if (!next.Equals("*", StringComparison.Ordinal))
                {
                    var parsedTarget = TryParseNullableInt(next);
                    if (parsedTarget.HasValue)
                    {
                        tgtUnitId = parsedTarget.Value;
                        UnitIdsSeen.Add(tgtUnitId.Value);
                        i++;

                        if (TryReadUnitBlock(f, ref i, out var tp, out var tpos))
                        {
                            tgtPools = tp;
                            tgtPos = tpos;

                            AddResourceSample(tgtUnitId.Value, sec, tp);
                        }
                    }
                }
            }

            // --- Aggregations (same logic as before) ---------------------------------

            if (dmg > 0)
            {
                AddToDict(_dmgPerSecond, sec, dmg);
                AddNested(_dmgDoneByUnitAbility, srcUnitId, abilityId, dmg);
                AddTotals(srcUnitId, dd: dmg);

                if (tgtUnitId.HasValue)
                {
                    AddNested(_dmgTakenByUnitAbility, tgtUnitId.Value, abilityId, dmg);
                    AddTotals(tgtUnitId.Value, dt: dmg);
                }
            }

            if (heal > 0)
            {
                AddToDict(_healPerSecond, sec, heal);
                AddNested(_healDoneByUnitAbility, srcUnitId, abilityId, heal);
                AddTotals(srcUnitId, hd: heal);

                if (tgtUnitId.HasValue)
                {
                    AddNested(_healTakenByUnitAbility, tgtUnitId.Value, abilityId, heal);
                    AddTotals(tgtUnitId.Value, ht: heal);
                }
            }

            // v34: richer aggregations for filtering + crit/hits + overheal (best-effort)
            var tgtKey = tgtUnitId ?? 0;
            var isCrit = result.Contains("CRITICAL", StringComparison.OrdinalIgnoreCase);

            // Best-effort overheal calculation: only meaningful for heals when we have target pools.
            long overhealCalc = 0;
            if (heal > 0 && tgtPools is Pools tp2)
            {
                var missing = Math.Max(0, (long)tp2.HealthMax - tp2.HealthCur);
                overhealCalc = Math.Max(0, heal - missing);
            }

            if (dmg > 0)
            {
                UpdateAgg(_damageAgg, srcUnitId, tgtKey, abilityId, total: dmg, overheal: 0, hits: 1, crits: isCrit ? 1 : 0, sec: sec);
            }

            if (heal > 0)
            {
                UpdateAgg(_healAgg, srcUnitId, tgtKey, abilityId, total: heal, overheal: overhealCalc, hits: 1, crits: isCrit ? 1 : 0, sec: sec);
            }

            // Resource change events (POWER_ENERGIZE / POWER_DRAIN / etc.)
            if (result.Contains("ENERGIZE", StringComparison.OrdinalIgnoreCase) ||
                result.Contains("DRAIN", StringComparison.OrdinalIgnoreCase))
            {
                var amount = dmg; // ESO logs usually use dmg-field as "amount" for energize/drain
                if (amount != 0)
                {
                    if (result.Contains("DRAIN", StringComparison.OrdinalIgnoreCase))
                        amount = -Math.Abs(amount);
                    else
                        amount = Math.Abs(amount);

                    var receiver = tgtUnitId ?? (srcUnitId != int.MinValue ? srcUnitId : (int?)null);
                    if (receiver.HasValue)
                    {
                        var kind = MapPowerTypeToKind(powerType);
                        _resourceEvents.Add(new ResourceEvent(relMs, kind, receiver.Value, srcUnitId, abilityId, amount, result));

                        // Keep ResourceGained totals as positive only.
                        if (amount > 0)
                        {
                            AddNested(_resourceByUnitAbility, receiver.Value, abilityId, amount);
                            AddTotals(receiver.Value, rg: amount);
                        }
                    }
                }
            }

            // Deaths (best-effort)
            if (result.Equals("KILLING_BLOW", StringComparison.OrdinalIgnoreCase) && tgtUnitId.HasValue)
            {
                _deaths.Add(new DeathEvent(relMs, tgtUnitId.Value, srcUnitId, abilityId, dmg, result));
                AddTotals(tgtUnitId.Value, deaths: 1);
            }
            else if ((result.Equals("DIED", StringComparison.OrdinalIgnoreCase) || result.Equals("UNIT_DIED", StringComparison.OrdinalIgnoreCase))
                     && srcUnitId != int.MinValue)
            {
                _deaths.Add(new DeathEvent(relMs, srcUnitId, tgtUnitId ?? int.MinValue, abilityId, dmg, result));
                AddTotals(srcUnitId, deaths: 1);
            }

            // --- Optional: store full parsed data for later (map visualizations etc.)
            // v36: store a compact sample for per-second filtered timelines.
            // TargetUnitId uses 0 when missing so it's easy to filter (and matches our AggKey convention).
            if (dmg > 0 || heal > 0)
            {
                _combatSamples.Add(new CombatSample(
                    RelMs: relMs,
                    SourceUnitId: srcUnitId,
                    TargetUnitId: tgtUnitId ?? 0,
                    AbilityId: abilityId,
                    Damage: dmg,
                    Heal: heal,
                    Overheal: overhealCalc,
                    IsCrit: isCrit,
                    Result: result
                ));
            }
        }

        private static void UpdateAgg(
            Dictionary<AggKey, AggMutable> dict,
            int sourceUnitId,
            int targetUnitId,
            int abilityId,
            long total,
            long overheal,
            int hits,
            int crits,
            int sec)
        {
            if (sourceUnitId == int.MinValue || abilityId == int.MinValue)
                return;

            var key = new AggKey(sourceUnitId, targetUnitId, abilityId);
            if (!dict.TryGetValue(key, out var m))
            {
                m = new AggMutable();
                dict[key] = m;
            }

            m.Total += total;
            m.Overheal += overheal;
            m.Hits += hits;
            m.Crits += crits;

            // seconds are monotonically increasing; last-second check gives us unique seconds per key.
            if (sec != m.LastSecond)
            {
                m.ActiveSeconds++;
                m.LastSecond = sec;
            }
        }


        public void AddEffectChanged(long relMs, IReadOnlyList<string> f, HashSet<int> hardModeAbilityIds)
        {
            // Example:
            // rel, EFFECT_CHANGED, GAINED/UPDATED/FADED, effectSlot, effectInstanceId, abilityId, targetUnitId,
            // health, magicka, stamina, ultimate, (more...), x, y, z, *
            var changeType = Get(f, 2);

            var effectSlot = TryParseInt(Get(f, 3));
            var effectInstanceId = TryParseLong(Get(f, 4));
            var abilityId = TryParseInt(Get(f, 5));
            var targetUnitId = TryParseInt(Get(f, 6));

            if (abilityId == int.MinValue || targetUnitId == int.MinValue)
                return;

            UnitIdsSeen.Add(targetUnitId);

            // Pools (best-effort) at fixed indices 7..10 (cur/max tokens)
            int? hCur = null, hMax = null, mCur = null, mMax = null, sCur = null, sMax = null, uCur = null, uMax = null;

            if (TryParsePoolToken(Get(f, 7), out var hc, out var hm)) { hCur = hc; hMax = hm; }
            if (TryParsePoolToken(Get(f, 8), out var mc, out var mm)) { mCur = mc; mMax = mm; }
            if (TryParsePoolToken(Get(f, 9), out var sc, out var sm)) { sCur = sc; sMax = sm; }
            if (TryParsePoolToken(Get(f, 10), out var uc, out var um)) { uCur = uc; uMax = um; }

            // Position (best-effort) usually at 13..15
            double? x = null, y = null, z = null;
            if (f.Count > 15)
            {
                x = TryParseDouble(Get(f, 13));
                y = TryParseDouble(Get(f, 14));
                z = TryParseDouble(Get(f, 15));
            }

            // Store full event (future-proof)
            _effectChanges.Add(new EffectChangedEvent(
                RelMs: relMs,
                ChangeType: changeType,
                EffectSlot: effectSlot == int.MinValue ? 0 : effectSlot,
                EffectInstanceId: effectInstanceId,
                AbilityId: abilityId,
                TargetUnitId: targetUnitId,
                HealthCur: hCur, HealthMax: hMax,
                MagickaCur: mCur, MagickaMax: mMax,
                StaminaCur: sCur, StaminaMax: sMax,
                UltimateCur: uCur, UltimateMax: uMax,
                X: x, Y: y, Z: z,
                Fields: f.ToArray()
            ));

            // ---- Uptime logic (GAINED / UPDATED / FADED) ----
            var key = new EffectKey(targetUnitId, abilityId);

            if (changeType.Equals("GAINED", StringComparison.OrdinalIgnoreCase))
            {
                if (!_openEffects.ContainsKey(key))
                    _openEffects[key] = relMs;

                var up = GetUptimeMutable(targetUnitId, abilityId);
                up.Applications++;

                if (hardModeAbilityIds.Contains(abilityId))
                    IsHardMode = true;
            }
            else if (changeType.Equals("UPDATED", StringComparison.OrdinalIgnoreCase))
            {
                // UPDATED = refresh/stack update. For uptime:
                // - if we missed GAINED, start it now
                if (!_openEffects.ContainsKey(key))
                    _openEffects[key] = relMs;

                // count as another application/refresh
                var up = GetUptimeMutable(targetUnitId, abilityId);
                up.Applications++;

                if (hardModeAbilityIds.Contains(abilityId))
                    IsHardMode = true;
            }
            else if (changeType.Equals("FADED", StringComparison.OrdinalIgnoreCase))
            {
                if (_openEffects.TryGetValue(key, out var start))
                {
                    _openEffects.Remove(key);
                    var dur = Math.Max(0, relMs - start);
                    var up = GetUptimeMutable(targetUnitId, abilityId);
                    up.TotalMs += dur;
                }
            }
        }

        public void AddBeginCast(long relMs, IReadOnlyList<string> f)
        {
            // Example:
            // rel, BEGIN_CAST, 0, F, castInstanceId, abilityId, casterUnitId,
            // health, magicka, stamina, ultimate, ???, ???, x, y, z, *
            var castInstanceId = TryParseLong(Get(f, 4));
            var abilityId = TryParseInt(Get(f, 5));
            var casterUnitId = TryParseInt(Get(f, 6));

            if (castInstanceId == 0 || abilityId == int.MinValue || casterUnitId == int.MinValue)
                return;

            UnitIdsSeen.Add(casterUnitId);

            Pools? pools = null;
            if (TryParsePoolsBlock(f, 7, out var p)) // health/mag/stam/ult are at 7..10
                pools = p;

            double? x = null, y = null, z = null;
            if (f.Count > 15) // x,y,z at 13..15 in your example
            {
                x = TryParseDouble(Get(f, 13));
                y = TryParseDouble(Get(f, 14));
                z = TryParseDouble(Get(f, 15));
            }

            var key = new CastKey(castInstanceId, abilityId);
            _openCasts[key] = new OpenCast(
                StartRelMs: relMs,
                CasterUnitId: casterUnitId,
                Pools: pools,
                X: x, Y: y, Z: z,
                BeginFields: f.ToArray()
            );
        }


        public void AddEndCast(long relMs, IReadOnlyList<string> f)
        {
            // rel, END_CAST, result, castInstanceId, abilityId
            var result = Get(f, 2);
            var castInstanceId = TryParseLong(Get(f, 3));
            var abilityId = TryParseInt(Get(f, 4));

            if (castInstanceId == 0 || abilityId == int.MinValue)
                return;

            var key = new CastKey(castInstanceId, abilityId);
            if (_openCasts.TryGetValue(key, out var open))
            {
                _openCasts.Remove(key);

                _casts.Add(new CastEntry(
                    StartRelMs: open.StartRelMs,
                    EndRelMs: relMs,
                    CasterUnitId: open.CasterUnitId,
                    AbilityId: abilityId,
                    Result: result,

                    CastInstanceId: castInstanceId,

                    HealthCur: open.Pools?.HealthCur, HealthMax: open.Pools?.HealthMax,
                    MagickaCur: open.Pools?.MagickaCur, MagickaMax: open.Pools?.MagickaMax,
                    StaminaCur: open.Pools?.StaminaCur, StaminaMax: open.Pools?.StaminaMax,
                    UltimateCur: open.Pools?.UltimateCur, UltimateMax: open.Pools?.UltimateMax,

                    X: open.X, Y: open.Y, Z: open.Z,

                    BeginFields: open.BeginFields,
                    EndFields: f.ToArray()
                ));

                AddTotals(open.CasterUnitId, casts: 1);
            }
            else
            {
                // Optional: orphan END_CAST (BEGIN_CAST missing) -> store minimal info
                _casts.Add(new CastEntry(
                    StartRelMs: relMs,
                    EndRelMs: relMs,
                    CasterUnitId: int.MinValue,
                    AbilityId: abilityId,
                    Result: result,
                    CastInstanceId: castInstanceId,
                    EndFields: f.ToArray()
                ));
            }
        }


        public void CloseOpen(long relMs)
        {
            // close open effects at fight end
            foreach (var kv in _openEffects.ToList())
            {
                var key = kv.Key;
                var start = kv.Value;
                var dur = Math.Max(0, relMs - start);
                var up = GetUptimeMutable(key.TargetUnitId, key.AbilityId);
                up.TotalMs += dur;
            }
            _openEffects.Clear();

            // close casts (unfinished)
            foreach (var kv in _openCasts.ToList())
            {
                var k = kv.Key;
                var open = kv.Value;
                _casts.Add(new CastEntry(open.StartRelMs, null, open.CasterUnitId, k.AbilityId, "OPEN"));
            }
            _openCasts.Clear();
        }

        public List<FightSeriesPoint> BuildSeries()
        {
            var maxSec = 0;
            if (_dmgPerSecond.Count > 0) maxSec = Math.Max(maxSec, _dmgPerSecond.Keys.Max());
            if (_healPerSecond.Count > 0) maxSec = Math.Max(maxSec, _healPerSecond.Keys.Max());

            var list = new List<FightSeriesPoint>(maxSec + 1);
            for (int s = 0; s <= maxSec; s++)
            {
                _dmgPerSecond.TryGetValue(s, out var dmg);
                _healPerSecond.TryGetValue(s, out var heal);
                list.Add(new FightSeriesPoint(s, dmg, heal));
            }

            return list;
        }

        public void AddHealthRegen(long relMs, IReadOnlyList<string> f)
        {
            // Beispiel:
            // 4350127,HEALTH_REGEN,1098,51,38785/43528,15937/15937,1649/22251,158/500,1000/1000,0,0.5026,0.4545,4.0417

            var unitId = TryParseInt(Get(f, 2));
            var regen = TryParseInt(Get(f, 3));
            if (unitId == int.MinValue) return;

            UnitIdsSeen.Add(unitId);

            // Pools (wir parsen 5 "cur/max" Paare)
            TryParsePool(Get(f, 4), out var hCur, out var hMax);
            TryParsePool(Get(f, 5), out var mCur, out var mMax);
            TryParsePool(Get(f, 6), out var sCur, out var sMax);
            TryParsePool(Get(f, 7), out var uCur, out var uMax);
            TryParsePool(Get(f, 8), out var spCur, out var spMax); // z.B. 0/1000

            var unk0 = TryParseInt(Get(f, 9));
            var x = TryParseFloat(Get(f, 10));
            var y = TryParseFloat(Get(f, 11));
            var z = TryParseFloat(Get(f, 12));

            _healthRegens.Add(new HealthRegenEvent(
                RelMs: relMs,
                UnitId: unitId,
                Regen: regen == int.MinValue ? 0 : regen,
                HealthCur: hCur, HealthMax: hMax,
                MagickaCur: mCur, MagickaMax: mMax,
                StaminaCur: sCur, StaminaMax: sMax,
                UltimateCur: uCur, UltimateMax: uMax,
                SpecialCur: spCur, SpecialMax: spMax,
                Unknown0: unk0 == int.MinValue ? 0 : unk0,
                X: x, Y: y, Z: z,
                RawFields: f.ToArray()
            ));

            // Optional: für Resources-Timeline kannst du hier zusätzlich dein "AddResourceSample" füttern,
            // falls du Health/Magicka/Stamina/Ultimate aus HEALTH_REGEN ebenfalls als Snapshot verwenden willst.
            // AddResourceSample(unitId, (int)((relMs - StartRelMs)/1000), new Pools(hCur,hMax,mCur,mMax,sCur,sMax,uCur,uMax));
        }

        public FightDetail BuildDetail(Dictionary<int, UnitInfo> units, Dictionary<int, EffectDefinition> effects)
        {
            var friendly = new List<int>();
            var enemy = new List<int>();

            foreach (var id in UnitIdsSeen)
            {
                if (!units.TryGetValue(id, out var u)) continue;

                if (IsFriendly(u)) friendly.Add(id);
                else if (IsEnemy(u)) enemy.Add(id);
            }

            friendly.Sort();
            enemy.Sort();

            // Convert mutable uptimes to read-only
            var uptimeByTarget = new Dictionary<int, IReadOnlyDictionary<int, EffectUptime>>();
            foreach (var t in _uptimes)
            {
                uptimeByTarget[t.Key] = t.Value.ToDictionary(
                    k => k.Key,
                    k => new EffectUptime(k.Value.TotalMs, k.Value.Applications));
            }

            // Convert per-second resource snapshots to read-only
            var resourcesByUnit = new Dictionary<int, IReadOnlyList<ResourcePoint>>();
            foreach (var u in _resourcesByUnitSecond)
            {
                var list = u.Value
                    .OrderBy(k => k.Key)
                    .Select(kv => kv.Value.ToRecord(kv.Key))
                    .ToList();
                resourcesByUnit[u.Key] = list;
            }

            // v34: materialize richer per-(src,tgt,ability) aggregations
            var damageAggs = _damageAgg
                .Select(kv => new CombatAgg(
                    SourceUnitId: kv.Key.SourceUnitId,
                    TargetUnitId: kv.Key.TargetUnitId,
                    AbilityId: kv.Key.AbilityId,
                    Total: kv.Value.Total,
                    Hits: kv.Value.Hits,
                    Crits: kv.Value.Crits,
                    ActiveSeconds: kv.Value.ActiveSeconds,
                    Overheal: 0))
                .ToList();

            var healAggs = _healAgg
                .Select(kv => new CombatAgg(
                    SourceUnitId: kv.Key.SourceUnitId,
                    TargetUnitId: kv.Key.TargetUnitId,
                    AbilityId: kv.Key.AbilityId,
                    Total: kv.Value.Total,
                    Hits: kv.Value.Hits,
                    Crits: kv.Value.Crits,
                    ActiveSeconds: kv.Value.ActiveSeconds,
                    Overheal: kv.Value.Overheal))
                .ToList();

            return new FightDetail(
                FightId: FightId,
                FriendlyUnitIds: friendly,
                EnemyUnitIds: enemy,
                TotalsByUnit: _totals.ToDictionary(k => k.Key, k => k.Value.ToRecord()),
                DamageDoneByUnitAbility: ToReadOnlyNested(_dmgDoneByUnitAbility),
                DamageTakenByUnitAbility: ToReadOnlyNested(_dmgTakenByUnitAbility),
                HealingDoneByUnitAbility: ToReadOnlyNested(_healDoneByUnitAbility),
                HealingTakenByUnitAbility: ToReadOnlyNested(_healTakenByUnitAbility),
                ResourceGainedByUnitAbility: ToReadOnlyNested(_resourceByUnitAbility),
                ResourcesByUnit: resourcesByUnit,
                ResourceEvents: _resourceEvents.ToList(),
                EffectUptimesByTarget: uptimeByTarget,
                Casts: _casts.ToList(),
                Deaths: _deaths.ToList(),
                UnhandledEventTypes: new Dictionary<string, int>(_unhandled),
                EffectChanges: _effectChanges.ToList(),
                HealthRegens: _healthRegens.ToList(),
                DamageAggs: damageAggs,
                HealAggs: healAggs,
                CombatSamples: _combatSamples.ToList()
            );
        }

        private void AddResourceSample(int unitId, int second, Pools pools)
        {
            if (!_resourcesByUnitSecond.TryGetValue(unitId, out var bySecond))
            {
                bySecond = new Dictionary<int, ResourcePointMutable>();
                _resourcesByUnitSecond[unitId] = bySecond;
            }

            // Keep the last snapshot for the second.
            if (!bySecond.TryGetValue(second, out var m))
            {
                m = new ResourcePointMutable();
                bySecond[second] = m;
            }

            m.Health = pools.HealthCur;
            m.HealthMax = pools.HealthMax;
            m.Magicka = pools.MagickaCur;
            m.MagickaMax = pools.MagickaMax;
            m.Stamina = pools.StaminaCur;
            m.StaminaMax = pools.StaminaMax;
            m.Ultimate = pools.UltimateCur;
            m.UltimateMax = pools.UltimateMax;
        }

        private static ResourceKind MapPowerTypeToKind(int powerType)
        {
            // ESO constants have changed over time; accept both old/new values.
            return powerType switch
            {
                -2 or 32 => ResourceKind.Health,
                0 or 1 => ResourceKind.Magicka,
                6 or 4 => ResourceKind.Stamina,
                10 or 8 => ResourceKind.Ultimate,
                _ => ResourceKind.Unknown
            };
        }

        private static bool TryParsePoolsBlock(IReadOnlyList<string> f, int startIndex, out Pools pools)
        {
            pools = default;

            // Expect 4 consecutive "cur/max" tokens: health, magicka, stamina, ultimate.
            if (startIndex < 0 || startIndex + 3 >= f.Count)
                return false;

            if (!TryParsePool(Get(f, startIndex + 0), out var hCur, out var hMax)) return false;
            if (!TryParsePool(Get(f, startIndex + 1), out var mCur, out var mMax)) return false;
            if (!TryParsePool(Get(f, startIndex + 2), out var sCur, out var sMax)) return false;
            if (!TryParsePool(Get(f, startIndex + 3), out var uCur, out var uMax)) return false;

            pools = new Pools(hCur, hMax, mCur, mMax, sCur, sMax, uCur, uMax);
            return true;
        }

        private static bool TryParsePool(string token, out int cur, out int max)
        {
            cur = 0;
            max = 0;
            token = token.Trim();

            var slash = token.IndexOf('/');
            if (slash <= 0 || slash >= token.Length - 1)
                return false;

            if (!int.TryParse(token.AsSpan(0, slash), NumberStyles.Integer, CultureInfo.InvariantCulture, out cur))
                return false;
            if (!int.TryParse(token.AsSpan(slash + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out max))
                return false;

            return true;
        }

        private EffectUptimeMutable GetUptimeMutable(int targetUnitId, int abilityId)
        {
            if (!_uptimes.TryGetValue(targetUnitId, out var map))
            {
                map = new Dictionary<int, EffectUptimeMutable>();
                _uptimes[targetUnitId] = map;
            }

            if (!map.TryGetValue(abilityId, out var up))
            {
                up = new EffectUptimeMutable();
                map[abilityId] = up;
            }

            return up;
        }

        private void AddTotals(int unitId, long dd = 0, long dt = 0, long hd = 0, long ht = 0, long rg = 0, int deaths = 0, int casts = 0)
        {
            if (unitId == int.MinValue) return;

            if (!_totals.TryGetValue(unitId, out var t))
            {
                t = new UnitTotalsMutable();
                _totals[unitId] = t;
            }

            t.DamageDone += dd;
            t.DamageTaken += dt;
            t.HealingDone += hd;
            t.HealingTaken += ht;
            t.ResourceGained += rg;
            t.Deaths += deaths;
            t.Casts += casts;
        }

        private static bool IsFriendly(UnitInfo u)
            => u.UnitType.Equals("PLAYER", StringComparison.OrdinalIgnoreCase)
               || u.Disposition.Contains("PLAYER_ALLY", StringComparison.OrdinalIgnoreCase)
               || u.Disposition.Contains("NPC_ALLY", StringComparison.OrdinalIgnoreCase)
               || u.Disposition.Contains("FRIENDLY", StringComparison.OrdinalIgnoreCase);

        private static bool IsEnemy(UnitInfo u)
            => u.Disposition.Contains("HOSTILE", StringComparison.OrdinalIgnoreCase);

        private static void AddToDict(Dictionary<int, long> dict, int key, long add)
        {
            dict.TryGetValue(key, out var cur);
            dict[key] = cur + add;
        }

        private static void AddNested(Dictionary<int, Dictionary<int, long>> dict, int k1, int k2, long add)
        {
            if (k1 == int.MinValue || k2 == int.MinValue) return;
            if (!dict.TryGetValue(k1, out var inner))
            {
                inner = new Dictionary<int, long>();
                dict[k1] = inner;
            }
            inner.TryGetValue(k2, out var cur);
            inner[k2] = cur + add;
        }

        private static IReadOnlyDictionary<int, IReadOnlyDictionary<int, long>> ToReadOnlyNested(Dictionary<int, Dictionary<int, long>> dict)
            => dict.ToDictionary(k => k.Key, k => (IReadOnlyDictionary<int, long>)new Dictionary<int, long>(k.Value));

        private static string Get(IReadOnlyList<string> f, int i) => i >= 0 && i < f.Count ? f[i] : "";

        private static long TryParseLong(string s)
            => long.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;

        private static int TryParseInt(string s)
            => int.TryParse(s.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : int.MinValue;

        private static bool TryParsePoolToken(string token, out int cur, out int max)
        {
            cur = 0; max = 0;
            token = (token ?? "").Trim();

            var slash = token.IndexOf('/');
            if (slash <= 0 || slash >= token.Length - 1) return false;

            if (!int.TryParse(token.AsSpan(0, slash), NumberStyles.Integer, CultureInfo.InvariantCulture, out cur))
                return false;
            if (!int.TryParse(token.AsSpan(slash + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out max))
                return false;

            return true;
        }

        private static double? TryParseDouble(string s)
        {
            if (double.TryParse((s ?? "").Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var v))
                return v;
            return 0f;
        }
        private static float TryParseFloat(string s)
            => float.TryParse(s.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : 0f;

        private static bool TryReadUnitBlock(IReadOnlyList<string> f, ref int i, out Pools pools, out WorldPos pos)
        {
            pools = default;
            pos = default;

            // Need at least 4 pool tokens (health, magicka, stamina, ultimate)
            if (i + 3 >= f.Count) return false;

            if (!TryParsePool(Get(f, i++), out var hCur, out var hMax)) return false;
            if (!TryParsePool(Get(f, i++), out var mCur, out var mMax)) return false;
            if (!TryParsePool(Get(f, i++), out var sCur, out var sMax)) return false;
            if (!TryParsePool(Get(f, i++), out var uCur, out var uMax)) return false;

            // There can be extra pool-like tokens after ultimate (e.g. "0/1000"). We don't rely on them,
            // but we must consume them so parsing reaches the numeric+xyz part.
            // Consume up to 2 extra "cur/max" tokens if present.
            for (int k = 0; k < 2 && i < f.Count; k++)
            {
                var t = Get(f, i).Trim();
                if (t.Contains('/', StringComparison.Ordinal) && TryParsePool(t, out _, out _))
                    i++;
                else
                    break;
            }

            // Next is usually a single integer (often 0) before xyz
            if (i < f.Count && long.TryParse(Get(f, i).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
                i++;

            // xyz (3 floats)
            if (i + 2 >= f.Count) return false;
            if (!double.TryParse(Get(f, i++).Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var x)) return false;
            if (!double.TryParse(Get(f, i++).Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var y)) return false;
            if (!double.TryParse(Get(f, i++).Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var z)) return false;

            pools = new Pools(hCur, hMax, mCur, mMax, sCur, sMax, uCur, uMax);
            pos = new WorldPos(x, y, z);
            return true;
        }


        private sealed class UnitTotalsMutable
        {
            public long DamageDone;
            public long DamageTaken;
            public long HealingDone;
            public long HealingTaken;
            public long ResourceGained;
            public int Deaths;
            public int Casts;

            public UnitCombatTotals ToRecord()
                => new(DamageDone, DamageTaken, HealingDone, HealingTaken, ResourceGained, Deaths, Casts);
        }

        private sealed class EffectUptimeMutable
        {
            public long TotalMs;
            public int Applications;
        }

        private sealed class ResourcePointMutable
        {
            public int Health;
            public int HealthMax;
            public int Magicka;
            public int MagickaMax;
            public int Stamina;
            public int StaminaMax;
            public int Ultimate;
            public int UltimateMax;

            public ResourcePoint ToRecord(int second)
                => new(second, Health, HealthMax, Magicka, MagickaMax, Stamina, StaminaMax, Ultimate, UltimateMax);
        }

        private readonly record struct Pools(
            int HealthCur, int HealthMax,
            int MagickaCur, int MagickaMax,
            int StaminaCur, int StaminaMax,
            int UltimateCur, int UltimateMax);

        private readonly record struct EffectKey(int TargetUnitId, int AbilityId);

        private readonly record struct CastKey(long CastInstanceId, int AbilityId);

        private sealed record OpenCast(
            long StartRelMs,
            int CasterUnitId,
            Pools? Pools,
            double? X,
            double? Y,
            double? Z,
            IReadOnlyList<string> BeginFields
        );

        private readonly record struct WorldPos(double X, double Y, double Z);

        private readonly record struct AggKey(int SourceUnitId, int TargetUnitId, int AbilityId);

        private sealed class AggMutable
        {
            public long Total;
            public long Overheal;
            public int Hits;
            public int Crits;
            public int ActiveSeconds;
            public int LastSecond = int.MinValue;
        }

    }
}
