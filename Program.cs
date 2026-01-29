// Program.cs
// .NET 8
// Downloads Sentinel-2 L2A RGB bands (B02/B03/B04) for the lowest-cloud scene
// in a given month over your bbox, using Planetary Computer STAC + SAS signing.
//
// Notes:
// - STAC endpoint: https://planetarycomputer.microsoft.com/api/stac/v1/search  (public)  :contentReference[oaicite:3]{index=3}
// - Sign endpoint pattern: https://planetarycomputer.microsoft.com/api/sas/v1/sign?href=... :contentReference[oaicite:4]{index=4}
// - Dataset: sentinel-2-l2a :contentReference[oaicite:5]{index=5}

using System.Net;
using System.Text;
using System.Text.Json;

var bbox = new double[]
{
    -103.86731513843112, // minLon (W)
    50.5123611,          // minLat (S)
    -102.9133333,        // maxLon (E)
    50.99259736981789    // maxLat (N)
};

var year = 2025;
var month = 5; // May
var cloudLt = 80; // lenient because you said some clouds are fine

var monthStart = new DateTime(year, month, 1);
var monthEnd = monthStart.AddMonths(1).AddDays(-1);

var projectRoot = FindProjectRoot(Environment.CurrentDirectory)
    ?? FindProjectRoot(AppContext.BaseDirectory)
    ?? Environment.CurrentDirectory;

var outDir = Path.Combine(projectRoot, "data", $"{year:D4}-{month:D2}");
Directory.CreateDirectory(outDir);

using var http = new HttpClient(new HttpClientHandler
{
    AutomaticDecompression = DecompressionMethods.All
});
http.Timeout = TimeSpan.FromMinutes(10);

// 1) STAC search
var searchUrl = "https://planetarycomputer.microsoft.com/api/stac/v1/search";

var payload = new
{
    collections = new[] { "sentinel-2-l2a" },
    bbox = bbox,
    datetime = $"{monthStart:yyyy-MM-dd}/{monthEnd:yyyy-MM-dd}",
    limit = 100,
    query = new Dictionary<string, object>
    {
        ["eo:cloud_cover"] = new Dictionary<string, object> { ["lt"] = cloudLt }
    }
};

var payloadJson = JsonSerializer.Serialize(payload);
var searchResp = await http.PostAsync(
    searchUrl,
    new StringContent(payloadJson, Encoding.UTF8, "application/json")
);
searchResp.EnsureSuccessStatusCode();

var searchBody = await searchResp.Content.ReadAsStringAsync();
var doc = JsonDocument.Parse(searchBody);

var features = doc.RootElement.GetProperty("features");
if (features.GetArrayLength() == 0)
{
    Console.WriteLine("No scenes found for that month/window.");
    return;
}

// pick lowest eo:cloud_cover
JsonElement? best = null;
double bestCloud = double.MaxValue;

foreach (var f in features.EnumerateArray())
{
    if (!f.TryGetProperty("properties", out var props)) continue;
    if (!props.TryGetProperty("eo:cloud_cover", out var cc)) continue;
    if (cc.ValueKind != JsonValueKind.Number) continue;

    var ccv = cc.GetDouble();
    if (ccv < bestCloud)
    {
        bestCloud = ccv;
        best = f;
    }
}

if (best is null)
{
    Console.WriteLine("Scenes returned, but none had eo:cloud_cover.");
    return;
}

var bestItem = best.Value;

var itemId = bestItem.GetProperty("id").GetString() ?? "(no id)";
Console.WriteLine($"Best scene: {itemId}  cloud={bestCloud:0.0}%");

await File.WriteAllTextAsync(Path.Combine(outDir, "item.json"), bestItem.GetRawText());

// 2) Download RGB band assets (B02/B03/B04)
// Each asset href needs signing (SAS) before download. :contentReference[oaicite:6]{index=6}
var assets = bestItem.GetProperty("assets");

await DownloadBandAsync("B02"); // Blue
await DownloadBandAsync("B03"); // Green
await DownloadBandAsync("B04"); // Red

Console.WriteLine("Done.");

async Task DownloadBandAsync(string bandKey)
{
    if (!assets.TryGetProperty(bandKey, out var asset))
    {
        Console.WriteLine($"Asset {bandKey} missing on item (unexpected).");
        return;
    }

    var href = asset.GetProperty("href").GetString();
    if (string.IsNullOrWhiteSpace(href))
    {
        Console.WriteLine($"Asset {bandKey} href missing.");
        return;
    }

    var signed = await SignHrefAsync(href);
    var filePath = Path.Combine(outDir, $"{bandKey}.tif");
    var tempPath = filePath + ".part";

    if (File.Exists(filePath) && new FileInfo(filePath).Length > 0)
    {
        Console.WriteLine($"{bandKey}: already downloaded.");
        return;
    }
    else if (File.Exists(filePath))
    {
        File.Delete(filePath);
    }

    var maxAttempts = 3;
    var baseDelay = TimeSpan.FromMinutes(2);

    for (var attempt = 1; attempt <= maxAttempts; attempt++)
    {
        try
        {
            Console.WriteLine($"{bandKey}: downloading (attempt {attempt}/{maxAttempts})...");
            using var resp = await http.GetAsync(signed, HttpCompletionOption.ResponseHeadersRead);
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

            Console.WriteLine($"{bandKey}: saved {new FileInfo(filePath).Length / 1024 / 1024.0:0.0} MB");
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
                Console.WriteLine($"{bandKey}: failed after {maxAttempts} attempts.");
                throw;
            }

            var delay = TimeSpan.FromMinutes(baseDelay.TotalMinutes * Math.Pow(2, attempt - 1));
            Console.WriteLine($"{bandKey}: error '{ex.Message}'. Waiting {delay.TotalMinutes:0} minutes before retry.");
            await Task.Delay(delay);
        }
    }
}

static async Task<string> SignHrefAsync(string href)
{
    // Observed/commonly used Planetary Computer signing endpoint:
    //   https://planetarycomputer.microsoft.com/api/sas/v1/sign?href={URLENCODED_HREF} :contentReference[oaicite:7]{index=7}
    // Returns JSON with a "href" containing the signed URL. :contentReference[oaicite:8]{index=8}
    var signEndpoint = "https://planetarycomputer.microsoft.com/api/sas/v1/sign?href=" + Uri.EscapeDataString(href);

    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(60);

    var resp = await http.GetAsync(signEndpoint);
    resp.EnsureSuccessStatusCode();

    var body = await resp.Content.ReadAsStringAsync();
    using var json = JsonDocument.Parse(body);

    if (json.RootElement.TryGetProperty("href", out var signedHref))
    {
        return signedHref.GetString() ?? throw new Exception("Signer returned null href.");
    }

    // Sometimes APIs return {"signedHref": "..."} etc. If this ever trips, we’ll adjust.
    throw new Exception("Signer response did not include 'href'. Body: " + body);
}

static string? FindProjectRoot(string startDir)
{
    var dir = new DirectoryInfo(startDir);
    while (dir is not null)
    {
        if (dir.EnumerateFiles("*.csproj", SearchOption.TopDirectoryOnly).Any())
        {
            return dir.FullName;
        }
        dir = dir.Parent;
    }

    return null;
}
