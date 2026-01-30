using System.Net;
using System.Text;
using System.Text.Json;

public sealed record StacItem(string Id, double? CloudCover, Dictionary<string, string> Assets, string RawJson);

public sealed class StacClient
{
    private readonly HttpClient _http;
    private const string SearchUrl = "https://planetarycomputer.microsoft.com/api/stac/v1/search";

    public StacClient(HttpClient http)
    {
        _http = http;
    }

    public async Task<List<StacItem>> SearchAsync(Bbox bbox, DateTime from, DateTime to, int cloudCoverMax, int limit)
    {
        var payload = new
        {
            collections = new[] { "sentinel-2-l2a" },
            bbox = bbox.ToArray(),
            datetime = $"{from:yyyy-MM-dd}/{to:yyyy-MM-dd}",
            limit = limit,
            query = new Dictionary<string, object>
            {
                ["eo:cloud_cover"] = new Dictionary<string, object> { ["lte"] = cloudCoverMax }
            }
        };

        var payloadJson = JsonSerializer.Serialize(payload);
        using var resp = await _http.PostAsync(SearchUrl, new StringContent(payloadJson, Encoding.UTF8, "application/json"));
        resp.EnsureSuccessStatusCode();

        var searchBody = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(searchBody);

        var features = doc.RootElement.GetProperty("features");
        var items = new List<StacItem>();

        foreach (var feature in features.EnumerateArray())
        {
            var id = feature.GetProperty("id").GetString() ?? "(no id)";

            double? cloud = null;
            if (feature.TryGetProperty("properties", out var props)
                && props.TryGetProperty("eo:cloud_cover", out var cc)
                && cc.ValueKind == JsonValueKind.Number)
            {
                cloud = cc.GetDouble();
            }

            var assets = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (feature.TryGetProperty("assets", out var assetObj))
            {
                foreach (var assetProp in assetObj.EnumerateObject())
                {
                    if (assetProp.Value.TryGetProperty("href", out var hrefEl))
                    {
                        var href = hrefEl.GetString();
                        if (!string.IsNullOrWhiteSpace(href))
                        {
                            assets[assetProp.Name] = href;
                        }
                    }
                }
            }

            items.Add(new StacItem(id, cloud, assets, feature.GetRawText()));
        }

        return items;
    }

    public async Task<string> SignHrefAsync(string href)
    {
        var signEndpoint = "https://planetarycomputer.microsoft.com/api/sas/v1/sign?href=" + Uri.EscapeDataString(href);
        using var resp = await _http.GetAsync(signEndpoint);
        resp.EnsureSuccessStatusCode();

        var body = await resp.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(body);

        if (json.RootElement.TryGetProperty("href", out var signedHref))
        {
            return signedHref.GetString() ?? throw new Exception("Signer returned null href.");
        }

        throw new Exception("Signer response did not include 'href'. Body: " + body);
    }
}
