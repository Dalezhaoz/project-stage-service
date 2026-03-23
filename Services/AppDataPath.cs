namespace ProjectStageService.Services;

public static class AppDataPath
{
    private const string PreferredWindowsPath = @"D:\其他软件\publish\data";

    public static string GetBaseDirectory()
    {
        if (OperatingSystem.IsWindows())
        {
            try
            {
                Directory.CreateDirectory(PreferredWindowsPath);
                return PreferredWindowsPath;
            }
            catch
            {
            }
        }

        var fallbackPath = Path.Combine(AppContext.BaseDirectory, "data");
        Directory.CreateDirectory(fallbackPath);
        return fallbackPath;
    }
}
