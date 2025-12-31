using System;
using System.Text;

namespace EsoLogViewer.Core.Parsing;

/// <summary>
/// Fast, allocation-light CSV tokenizer that supports quotes (") and escaped quotes ("").
/// It does not unescape backslash escapes (ESO logs don't use them).
/// </summary>
public static class CsvTokenizer
{
    public static List<string> Tokenize(string line)
    {
        var res = new List<string>(32);
        if (string.IsNullOrEmpty(line)) return res;

        var sb = new StringBuilder(line.Length);
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    // Escaped quote
                    sb.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (c == ',' && !inQuotes)
            {
                res.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            sb.Append(c);
        }

        res.Add(sb.ToString());
        return res;
    }


    /// <summary>
    /// Tokenize a line like CSV, but also treats bracketed lists ([...], [[...],...]) as single fields.
    /// Needed for PLAYER_INFO, where the log emits unquoted lists containing commas.
    /// </summary>
    public static List<string> TokenizeWithBrackets(string line)
    {
        var res = new List<string>(16);
        if (string.IsNullOrEmpty(line)) return res;

        var sb = new StringBuilder(line.Length);
        bool inQuotes = false;
        int bracketDepth = 0;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    sb.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
                continue;
            }

            if (!inQuotes)
            {
                if (c == '[') bracketDepth++;
                else if (c == ']') bracketDepth = Math.Max(0, bracketDepth - 1);

                if (c == ',' && bracketDepth == 0)
                {
                    res.Add(sb.ToString());
                    sb.Clear();
                    continue;
                }
            }

            sb.Append(c);
        }

        res.Add(sb.ToString());
        return res;
    }

}
