namespace EsoLogViewer.Core.Models;

public sealed record TrialRun(
    int TrialKey,                 // das "19" aus BEGIN_TRIAL/END_TRIAL (Bedeutung ggf. später)
    long StartRelMs,
    long EndRelMs,
    long StartUnixTimeMs,
    long EndUnixTimeMs,

    long DurationMs,              // aus END_TRIAL Feld #3 (reported)
    bool Success,                 // T/F
    long FinalScore,              // Feld #5
    int Vitality,                 // Feld #6 (z.B. 36000 => 36/36)

    IReadOnlyList<string> BeginFields,
    IReadOnlyList<string> EndFields
);