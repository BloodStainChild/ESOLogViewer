using BCnEncoder.Decoder;
using BCnEncoder.ImageSharp;
using EsoLogViewer.Core.Storage;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;

namespace EsoLogViewer.Core.Services;

/// <summary>
/// Copies ESO UI <c>.dds</c> icons from a source folder (containing an <c>esoui</c> directory)
/// into an application-local cache folder, converting them to PNG so the viewer can render them.
/// </summary>
public static class IconCacheBuilder
{
    public static async Task<IconCacheBuildResult> BuildAsync(
        string sourcePath,
        string cacheRoot,
        bool overwrite,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(sourcePath))
            throw new ArgumentException("Source path is empty.", nameof(sourcePath));
        if (string.IsNullOrWhiteSpace(cacheRoot))
            throw new ArgumentException("Cache root is empty.", nameof(cacheRoot));

        var (baseDir, esouiDir) = ResolveEsouiRoot(sourcePath);
        Directory.CreateDirectory(cacheRoot);

        var decoder = new BcDecoder();
        var total = 0;
        var converted = 0;
        var skipped = 0;
        var failed = 0;

        // Enumerate DDS files under esoui.
        foreach (var file in Directory.EnumerateFiles(esouiDir, "*.dds", SearchOption.AllDirectories))
        {
            ct.ThrowIfCancellationRequested();
            total++;

            var rel = Path.GetRelativePath(baseDir, file);
            // Normalise to the on-disk layout we serve under /usericons/...
            var dest = Path.Combine(cacheRoot, rel);
            dest = Path.ChangeExtension(dest, ".png");
            var destDir = Path.GetDirectoryName(dest);
            if (!string.IsNullOrWhiteSpace(destDir))
                Directory.CreateDirectory(destDir);

            if (!overwrite && File.Exists(dest))
            {
                skipped++;
                continue;
            }

            try
            {
                await using var fs = File.OpenRead(file);
                using Image<Rgba32> image = decoder.DecodeToImageRgba32(fs);

                await using var outFs = File.Open(dest, FileMode.Create, FileAccess.Write, FileShare.None);
                image.SaveAsPng(outFs);
                converted++;
            }
            catch
            {
                failed++;
            }
        }

        return new IconCacheBuildResult(cacheRoot, total, converted, skipped, failed);
    }

    private static (string BaseDir, string EsouiDir) ResolveEsouiRoot(string sourcePath)
    {
        sourcePath = Path.GetFullPath(sourcePath.Trim());

        // User may pass the 'esoui' directory itself, or a folder that contains it.
        if (Directory.Exists(sourcePath) && string.Equals(Path.GetFileName(sourcePath), "esoui", StringComparison.OrdinalIgnoreCase))
        {
            var baseDir = Directory.GetParent(sourcePath)?.FullName
                          ?? throw new InvalidOperationException("Unable to resolve parent folder for the given esoui path.");
            return (baseDir, sourcePath);
        }

        var esoui = Path.Combine(sourcePath, "esoui");
        if (Directory.Exists(esoui))
            return (sourcePath, esoui);

        throw new DirectoryNotFoundException(
            "The source path must be the 'esoui' folder, or a folder that contains an 'esoui' directory.");
    }
}
