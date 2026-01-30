using System.Net;

public sealed class BandDownloader
{
    private readonly HttpClient _http;
    private readonly StacClient _stacClient;

    public BandDownloader(HttpClient http, StacClient stacClient)
    {
        _http = http;
        _stacClient = stacClient;
    }

    public async Task DownloadBandsAsync(StacItem item, IReadOnlyCollection<string> bandKeys, string outputDir)
    {
        Directory.CreateDirectory(outputDir);

        foreach (var bandKey in bandKeys)
        {
            if (!item.Assets.TryGetValue(bandKey, out var href))
            {
                Console.WriteLine($"Asset {bandKey} missing on item {item.Id} (skipping).");
                continue;
            }

            var signed = await _stacClient.SignHrefAsync(href);
            var filePath = Path.Combine(outputDir, $"{bandKey}.tif");
            await DownloadFileWithRetryAsync(signed, filePath, bandKey);
        }
    }

    private async Task DownloadFileWithRetryAsync(string url, string filePath, string label)
    {
        var tempPath = filePath + ".part";
        if (File.Exists(filePath) && new FileInfo(filePath).Length > 0)
        {
            Console.WriteLine($"{label}: already downloaded.");
            return;
        }

        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }

        var maxAttempts = 3;
        var baseDelay = TimeSpan.FromMinutes(2);

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                Console.WriteLine($"{label}: downloading (attempt {attempt}/{maxAttempts})...");
                using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
                resp.EnsureSuccessStatusCode();

                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }

                await using (var fs = File.Create(tempPath))
                {
                    await resp.Content.CopyToAsync(fs);
                }

                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }

                File.Move(tempPath, filePath);

                var length = new FileInfo(filePath).Length;
                if (length <= 0)
                {
                    File.Delete(filePath);
                    throw new IOException("Downloaded file size was zero.");
                }

                Console.WriteLine($"{label}: saved {length / 1024 / 1024.0:0.0} MB");
                return;
            }
            catch (Exception ex)
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }

                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }

                if (attempt == maxAttempts)
                {
                    Console.WriteLine($"{label}: failed after {maxAttempts} attempts.");
                    throw;
                }

                var delay = TimeSpan.FromMinutes(baseDelay.TotalMinutes * Math.Pow(2, attempt - 1));
                Console.WriteLine($"{label}: error '{ex.Message}'. Waiting {delay.TotalMinutes:0} minutes before retry.");
                await Task.Delay(delay);
            }
        }
    }
}
