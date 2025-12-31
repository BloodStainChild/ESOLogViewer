namespace EsoLogViewer.Core.Models;

public sealed record HealthRegenEvent(
    long RelMs,
    int UnitId,
    int Regen,                 // z.B. "51"
    int HealthCur, int HealthMax,
    int MagickaCur, int MagickaMax,
    int StaminaCur, int StaminaMax,
    int UltimateCur, int UltimateMax,
    int SpecialCur, int SpecialMax, // das 0/1000 Feld (wir speichern es komplett)
    int Unknown0,               // das einzelne "0" vor den Koordinaten
    float X, float Y, float Z,
    IReadOnlyList<string> RawFields
);
