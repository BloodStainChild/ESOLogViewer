using System.Globalization;

namespace EsoLogViewer.Core.Services;

public sealed record BscAbilityRow(
    int AbilityId,
    string? Name,
    string? Texture,
    string? Description,
    string? DescHeader,
    int BuffType,
    bool IsUltimate,
    bool IsPassive,
    bool IsChanneled,
    double CastTime,
    double Cooldown,
    double Duration,
    double Radius,
    double MinRange,
    double MaxRange,

    // Extended metadata (v39): used for better grouping/UX (e.g. passive categories).
    int SkillLineIndex,
    int SkillLineId,
    int SkillAbilityIndex,
    int SkillRankIndex,
    int SkillLineRank,
    string? SkillLineName,
    bool SkillLineDiscovered,
    int SkillType,
    int SkillMorphChoice,
    string? SkillKeySource
);

public sealed record BscItemRow(
    int ItemId,
    string? Name,
    int? SetId,
    string? SetName,
    int ArmorType,
    int EquipType,
    int WeaponType,
    string? Icon
);

public sealed record BscSetRow(
    int SetId,
    string? Name
);

/// <summary>
/// Fast-ish reader for BSCLDCSavedVariables.lua.
/// Parses only the known SavedVariables shape:
///   BSCLDCSaved["Default"]["@..."]["$AccountWide"]{ abilities/items/sets }
/// 
/// This is not a full Lua parser; it supports the subset used by ESO SavedVariables.
/// </summary>
public sealed class BscSavedVarsReader
{
    private readonly LuaCursor _c;

    public BscSavedVarsReader(string luaText)
    {
        if (luaText is null) throw new ArgumentNullException(nameof(luaText));

        // Jump close to the actual assignment to avoid scanning huge headers.
        var idx = luaText.IndexOf("BSCLDCSaved", StringComparison.Ordinal);
        if (idx >= 0)
            luaText = luaText.Substring(idx);

        _c = new LuaCursor(luaText);
    }

    public IEnumerable<BscSetRow> ReadSets()
    {
        foreach (var aw in EnumerateAccountWideTables())
        {
            var r = aw.Clone();
            foreach (var row in ParseSets(r))
                yield return row;
        }
    }

    public IEnumerable<BscItemRow> ReadItems()
    {
        foreach (var aw in EnumerateAccountWideTables())
        {
            var r = aw.Clone();
            foreach (var row in ParseItems(r))
                yield return row;
        }
    }

    public IEnumerable<BscAbilityRow> ReadAbilities()
    {
        foreach (var aw in EnumerateAccountWideTables())
        {
            var r = aw.Clone();
            foreach (var row in ParseAbilities(r))
                yield return row;
        }
    }

    /// <summary>
    /// Finds each $AccountWide table in the SavedVariables file and returns a cursor positioned at its '{'.
    /// </summary>
    private IEnumerable<LuaCursor> EnumerateAccountWideTables()
    {
        var r = _c.Clone();

        // Find first table after '='.
        r.SkipWsAndComments();
        r.SeekChar('{');
        if (!r.TryConsume('{')) yield break;

        // Parse root table: look for ["Default"] only.
        foreach (var def in r.EnumerateTableEntries())
        {
            if (def.Key is not string sKey || !string.Equals(sKey, "Default", StringComparison.Ordinal))
            {
                def.Value.SkipValue();
                continue;
            }

            // Default table
            var d = def.Value;
            if (!d.TryConsume('{'))
            {
                d.SkipValue();
                continue;
            }

            foreach (var acc in d.EnumerateTableEntries())
            {
                // Each account table
                var accTbl = acc.Value;
                if (!accTbl.TryConsume('{'))
                {
                    accTbl.SkipValue();
                    continue;
                }

                foreach (var inner in accTbl.EnumerateTableEntries())
                {
                    if (inner.Key is string k && string.Equals(k, "$AccountWide", StringComparison.Ordinal))
                    {
                        // yield a cursor at the opening '{' of accountwide table
                        var aw = inner.Value;
                        aw.SkipWsAndComments();
                        aw.SeekChar('{');
                        yield return aw;
                    }
                    else
                    {
                        inner.Value.SkipValue();
                    }
                }

                accTbl.TryConsume('}');
            }

            d.TryConsume('}');
        }

        r.TryConsume('}');
    }

    private static IEnumerable<BscSetRow> ParseSets(LuaCursor aw)
    {
        foreach (var entry in EnumerateSectionTable(aw, "sets"))
        {
            var setId = entry.NumericKey;
            string? name = null;

            var t = entry.Value;
            if (!t.TryConsume('{')) { t.SkipValue(); continue; }
            foreach (var kv in t.EnumerateTableEntries())
            {
                if (kv.Key is string sk && sk == "name")
                    name = kv.Value.ReadAsString();
                else
                    kv.Value.SkipValue();
            }
            t.TryConsume('}');
            yield return new BscSetRow(setId, name);
        }
    }

    private static IEnumerable<BscItemRow> ParseItems(LuaCursor aw)
    {
        foreach (var entry in EnumerateSectionTable(aw, "items"))
        {
            var itemId = entry.NumericKey;

            string? name = null;
            int? setId = null;
            string? setName = null;
            int armorType = 0;
            int equipType = 0;
            int weaponType = 0;
            string? icon = null;

            var t = entry.Value;
            if (!t.TryConsume('{')) { t.SkipValue(); continue; }
            foreach (var kv in t.EnumerateTableEntries())
            {
                if (kv.Key is not string k) { kv.Value.SkipValue(); continue; }

                switch (k)
                {
                    case "name": name = kv.Value.ReadAsString(); break;
                    case "setId": setId = kv.Value.ReadAsIntNullable(); break;
                    case "setName": setName = kv.Value.ReadAsString(); break;
                    case "armorType": armorType = kv.Value.ReadAsInt(); break;
                    case "equipType": equipType = kv.Value.ReadAsInt(); break;
                    case "weaponType": weaponType = kv.Value.ReadAsInt(); break;
                    case "itemIcon": icon = kv.Value.ReadAsString(); break;
                    default: kv.Value.SkipValue(); break;
                }
            }
            t.TryConsume('}');

            yield return new BscItemRow(itemId, name, setId, setName, armorType, equipType, weaponType, icon);
        }
    }

    private static IEnumerable<BscAbilityRow> ParseAbilities(LuaCursor aw)
    {
        foreach (var entry in EnumerateSectionTable(aw, "abilities"))
        {
            var abilityId = entry.NumericKey;

            string? name = null;
            string? tex = null;
            string? desc = null;
            string? hdr = null;
            int buffType = 0;
            bool isUlt = false;
            bool isPassive = false;
            bool isChan = false;
            double castTime = 0;
            double cooldown = 0;
            double duration = 0;
            double radius = 0;
            double minRange = 0;
            double maxRange = 0;

            int skillLineIndex = 0;
            int skillLineId = 0;
            int skillAbilityIndex = 0;
            int skillRankIndex = 0;
            int skillLineRank = 0;
            string? skillLineName = null;
            bool skillLineDiscovered = false;
            int skillType = 0;
            int skillMorphChoice = 0;
            string? skillKeySource = null;

            var t = entry.Value;
            if (!t.TryConsume('{')) { t.SkipValue(); continue; }
            foreach (var kv in t.EnumerateTableEntries())
            {
                if (kv.Key is not string k) { kv.Value.SkipValue(); continue; }

                switch (k)
                {
                    case "name": name = kv.Value.ReadAsString(); break;
                    case "texture": tex = kv.Value.ReadAsString(); break;
                    case "description": desc = kv.Value.ReadAsString(); break;
                    case "descHeader": hdr = kv.Value.ReadAsString(); break;
                    case "buffType": buffType = kv.Value.ReadAsInt(); break;
                    case "isUltimate": isUlt = kv.Value.ReadAsBool(); break;
                    case "isPassive": isPassive = kv.Value.ReadAsBool(); break;
                    case "isChanneled": isChan = kv.Value.ReadAsBool(); break;
                    case "castTime": castTime = kv.Value.ReadAsDouble(); break;
                    case "cooldown": cooldown = kv.Value.ReadAsDouble(); break;
                    case "duration": duration = kv.Value.ReadAsDouble(); break;
                    case "radius": radius = kv.Value.ReadAsDouble(); break;
                    case "minRange": minRange = kv.Value.ReadAsDouble(); break;
                    case "maxRange": maxRange = kv.Value.ReadAsDouble(); break;

                    // Extended metadata
                    case "skillLineIndex": skillLineIndex = kv.Value.ReadAsInt(); break;
                    case "skillLineId": skillLineId = kv.Value.ReadAsInt(); break;
                    case "skillAbilityIndex": skillAbilityIndex = kv.Value.ReadAsInt(); break;
                    case "skillRankIndex": skillRankIndex = kv.Value.ReadAsInt(); break;
                    case "skillLineRank": skillLineRank = kv.Value.ReadAsInt(); break;
                    case "skillLineName": skillLineName = kv.Value.ReadAsString(); break;
                    case "skillLineDiscovered": skillLineDiscovered = kv.Value.ReadAsBool(); break;
                    case "skillType": skillType = kv.Value.ReadAsInt(); break;
                    case "skillMorphChoice": skillMorphChoice = kv.Value.ReadAsInt(); break;
                    case "skillKeySource": skillKeySource = kv.Value.ReadAsString(); break;
                    default:
                        kv.Value.SkipValue();
                        break;
                }
            }
            t.TryConsume('}');

            yield return new BscAbilityRow(
                abilityId,
                name,
                tex,
                desc,
                hdr,
                buffType,
                isUlt,
                isPassive,
                isChan,
                castTime,
                cooldown,
                duration,
                radius,
                minRange,
                maxRange,

                skillLineIndex,
                skillLineId,
                skillAbilityIndex,
                skillRankIndex,
                skillLineRank,
                skillLineName,
                skillLineDiscovered,
                skillType,
                skillMorphChoice,
                skillKeySource);
        }
    }

    private readonly struct SectionEntry
    {
        public required int NumericKey { get; init; }
        public required LuaCursor Value { get; init; }
    }

    private static IEnumerable<SectionEntry> EnumerateSectionTable(LuaCursor aw, string sectionKey)
    {
        // aw points at '{' of accountwide table
        var r = aw.Clone();
        if (!r.TryConsume('{')) yield break;

        foreach (var kv in r.EnumerateTableEntries())
        {
            if (kv.Key is not string k || !string.Equals(k, sectionKey, StringComparison.Ordinal))
            {
                kv.Value.SkipValue();
                continue;
            }

            // kv.Value is at the section value (should be a table)
            var sec = kv.Value;
            if (!sec.TryConsume('{'))
            {
                sec.SkipValue();
                continue;
            }

            foreach (var entry in sec.EnumerateTableEntries())
            {
                if (entry.Key is long nKey)
                {
                    yield return new SectionEntry { NumericKey = checked((int)nKey), Value = entry.Value };
                }
                else
                {
                    entry.Value.SkipValue();
                }
            }

            sec.TryConsume('}');
        }

        r.TryConsume('}');
    }

    /// <summary>
    /// Small cursor/lexer for the Lua subset ESO SavedVariables use.
    /// </summary>
    private sealed class LuaCursor
    {
        private readonly string _s;
        private int _i;

        public LuaCursor(string s) { _s = s; _i = 0; }

        public LuaCursor Clone() => new(_s) { _i = _i };

        public void SkipWsAndComments()
        {
            while (_i < _s.Length)
            {
                char c = _s[_i];
                if (char.IsWhiteSpace(c)) { _i++; continue; }

                if (c == '-' && _i + 1 < _s.Length && _s[_i + 1] == '-')
                {
                    // comment to end-of-line
                    _i += 2;
                    while (_i < _s.Length && _s[_i] != '\n') _i++;
                    continue;
                }

                break;
            }
        }

        public void SeekChar(char ch)
        {
            // Seek to next 'ch' outside of strings.
            bool inStr = false;
            for (; _i < _s.Length; _i++)
            {
                var c = _s[_i];
                if (c == '"')
                {
                    // toggle string, but skip escaped quotes
                    if (!inStr) inStr = true;
                    else
                    {
                        // check for escape
                        int backslashes = 0;
                        int j = _i - 1;
                        while (j >= 0 && _s[j] == '\\') { backslashes++; j--; }
                        if (backslashes % 2 == 0) inStr = false;
                    }
                }

                if (!inStr && c == ch) return;
            }
        }

        public bool TryConsume(char ch)
        {
            SkipWsAndComments();
            if (_i < _s.Length && _s[_i] == ch) { _i++; return true; }
            return false;
        }

        public IEnumerable<(object? Key, LuaCursor Value)> EnumerateTableEntries()
        {
            while (true)
            {
                SkipWsAndComments();
                if (_i >= _s.Length) yield break;
                if (_s[_i] == '}') yield break;

                // Try to parse key
                object? key = null;
                int mark = _i;

                if (_s[_i] == '[')
                {
                    _i++; // [
                    SkipWsAndComments();
                    if (_i < _s.Length && _s[_i] == '"')
                        key = ReadString();
                    else
                        key = ReadNumberAsLong();
                    SkipWsAndComments();
                    if (_i < _s.Length && _s[_i] == ']') _i++;
                    SkipWsAndComments();
                    if (_i < _s.Length && _s[_i] == '=') _i++;
                    else
                    {
                        // Not a key assignment, rewind.
                        _i = mark;
                        key = null;
                    }
                }
                else if (IsIdentStart(_s[_i]))
                {
                    var ident = ReadIdentifier();
                    SkipWsAndComments();
                    if (_i < _s.Length && _s[_i] == '=')
                    {
                        _i++;
                        key = ident;
                    }
                    else
                    {
                        // array element, rewind
                        _i = mark;
                        key = null;
                    }
                }

                var val = Clone();
                if (key is not null)
                {
                    // move our iterator cursor to the value start
                    SkipWsAndComments();
                    val = Clone();
                    // Advance this cursor past the value so next iteration continues.
                    SkipValue();
                }
                else
                {
                    // array element: value starts here
                    val = Clone();
                    SkipValue();
                }

                // consume optional ','
                SkipWsAndComments();
                if (_i < _s.Length && _s[_i] == ',') _i++;

                yield return (key, val);
            }
        }

        public void SkipValue()
        {
            SkipWsAndComments();
            if (_i >= _s.Length) return;
            char c = _s[_i];
            if (c == '{') { SkipTable(); return; }
            if (c == '"') { _ = ReadString(); return; }
            if (c == '-' || char.IsDigit(c)) { _ = ReadNumberToken(); return; }
            if (IsIdentStart(c)) { _ = ReadIdentifier(); return; }

            // unknown token; advance one char to avoid infinite loops
            _i++;
        }

        private void SkipTable()
        {
            // Assumes current token is '{'
            int depth = 0;
            bool inStr = false;
            while (_i < _s.Length)
            {
                char c = _s[_i++];
                if (c == '"')
                {
                    if (!inStr) inStr = true;
                    else
                    {
                        int backslashes = 0;
                        int j = _i - 2;
                        while (j >= 0 && _s[j] == '\\') { backslashes++; j--; }
                        if (backslashes % 2 == 0) inStr = false;
                    }
                    continue;
                }
                if (inStr) continue;
                if (c == '{') depth++;
                else if (c == '}')
                {
                    depth--;
                    if (depth <= 0) return;
                }
            }
        }

        public string? ReadAsString()
        {
            SkipWsAndComments();
            if (_i >= _s.Length) return null;
            if (_s[_i] == '"') return ReadString();
            if (IsIdentStart(_s[_i]))
            {
                var id = ReadIdentifier();
                if (id == "nil") return null;
                return id;
            }
            if (char.IsDigit(_s[_i]) || _s[_i] == '-')
                return ReadNumberToken();
            return null;
        }

        public int ReadAsInt() => ReadAsIntNullable() ?? 0;

        public int? ReadAsIntNullable()
        {
            SkipWsAndComments();
            if (_i >= _s.Length) return null;
            if (IsIdentStart(_s[_i]))
            {
                var id = ReadIdentifier();
                if (id == "nil") return null;
                if (int.TryParse(id, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv)) return iv;
                return null;
            }
            if (_s[_i] == '-' || char.IsDigit(_s[_i]))
            {
                var tok = ReadNumberToken();
                if (int.TryParse(tok, NumberStyles.Integer, CultureInfo.InvariantCulture, out var iv)) return iv;
                if (double.TryParse(tok, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv)) return (int)dv;
            }
            return null;
        }

        public double ReadAsDouble()
        {
            SkipWsAndComments();
            if (_i >= _s.Length) return 0;
            if (_s[_i] == '-' || char.IsDigit(_s[_i]))
            {
                var tok = ReadNumberToken();
                if (double.TryParse(tok, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv)) return dv;
                return 0;
            }
            if (IsIdentStart(_s[_i]))
            {
                var id = ReadIdentifier();
                if (double.TryParse(id, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv)) return dv;
            }
            return 0;
        }

        public bool ReadAsBool()
        {
            SkipWsAndComments();
            if (_i >= _s.Length) return false;
            if (IsIdentStart(_s[_i]))
            {
                var id = ReadIdentifier();
                return id == "true";
            }
            return false;
        }

        private string ReadIdentifier()
        {
            int start = _i;
            _i++;
            while (_i < _s.Length)
            {
                char c = _s[_i];
                if (!(char.IsLetterOrDigit(c) || c == '_' || c == '.')) break;
                _i++;
            }
            return _s.Substring(start, _i - start);
        }

        private string ReadString()
        {
            // assumes current char is '"'
            _i++; // skip opening
            var sb = new System.Text.StringBuilder();
            while (_i < _s.Length)
            {
                char c = _s[_i++];
                if (c == '"') break;
                if (c == '\\' && _i < _s.Length)
                {
                    char n = _s[_i++];
                    sb.Append(n switch
                    {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        _ => n
                    });
                    continue;
                }
                sb.Append(c);
            }
            return sb.ToString();
        }

        private string ReadNumberToken()
        {
            int start = _i;
            _i++;
            while (_i < _s.Length)
            {
                char c = _s[_i];
                if (!(char.IsDigit(c) || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-')) break;
                _i++;
            }
            return _s.Substring(start, _i - start);
        }

        private long ReadNumberAsLong()
        {
            var tok = ReadNumberToken();
            if (long.TryParse(tok, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v)) return v;
            if (double.TryParse(tok, NumberStyles.Float, CultureInfo.InvariantCulture, out var d)) return (long)d;
            return 0;
        }

        private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';
    }
}
