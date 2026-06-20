using System.Globalization;

public sealed record WorkImageCleanupResult(int FilesDeleted, int DirectoriesDeleted, IReadOnlyList<string> Errors)
{
    public bool HasWork => FilesDeleted > 0 || DirectoriesDeleted > 0 || Errors.Count > 0;
}

public static class WorkImageCleaner
{
    private static readonly HashSet<string> IntermediateExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".tif",
        ".tiff",
        ".vrt",
        ".raw",
        ".bin",
        ".hdr",
        ".dat",
        ".part"
    };

    private static readonly HashSet<string> ScratchDirectoryNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "_work",
        "_full-assets"
    };

    public static WorkImageCleanupResult Clean(string root)
    {
        if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
        {
            return new WorkImageCleanupResult(0, 0, Array.Empty<string>());
        }

        var filesDeleted = 0;
        var directoriesDeleted = 0;
        var errors = new List<string>();

        foreach (var directory in Directory
            .EnumerateDirectories(root, "*", SearchOption.AllDirectories)
            .OrderByDescending(path => path.Length))
        {
            if (ScratchDirectoryNames.Contains(Path.GetFileName(directory)))
            {
                TryDeleteDirectory(directory, ref directoriesDeleted, errors);
            }
        }

        foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            if (!IsIntermediateImageFile(file))
            {
                continue;
            }

            try
            {
                File.Delete(file);
                filesDeleted++;
            }
            catch (Exception ex)
            {
                errors.Add($"Unable to delete work image {file}: {ex.Message}");
            }
        }

        foreach (var directory in Directory
            .EnumerateDirectories(root, "*", SearchOption.AllDirectories)
            .OrderByDescending(path => path.Length))
        {
            TryDeleteEmptyDirectory(directory, ref directoriesDeleted, errors);
        }

        return new WorkImageCleanupResult(filesDeleted, directoriesDeleted, errors);
    }

    public static void LogResult(string root, WorkImageCleanupResult result)
    {
        if (!result.HasWork)
        {
            return;
        }

        Console.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Work image cleanup under {root}: deleted {result.FilesDeleted} file(s), {result.DirectoriesDeleted} folder(s)."));

        foreach (var error in result.Errors)
        {
            Console.WriteLine(error);
        }
    }

    private static bool IsIntermediateImageFile(string filePath)
    {
        var fileName = Path.GetFileName(filePath);
        if (fileName.EndsWith(".aux.xml", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return IntermediateExtensions.Contains(Path.GetExtension(filePath));
    }

    private static void TryDeleteDirectory(string directory, ref int directoriesDeleted, List<string> errors)
    {
        try
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
                directoriesDeleted++;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Unable to delete work folder {directory}: {ex.Message}");
        }
    }

    private static void TryDeleteEmptyDirectory(string directory, ref int directoriesDeleted, List<string> errors)
    {
        try
        {
            if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
            {
                Directory.Delete(directory);
                directoriesDeleted++;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Unable to delete empty work folder {directory}: {ex.Message}");
        }
    }
}
