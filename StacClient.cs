using System.Net;
using System.Globalization;
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
            datetime = BuildDateTimeInterval(from, to),
            limit = limit,
            query = new Dictionary<string, object>
            {
                ["eo:cloud_cover"] = new Dictionary<string, object> { ["lte"] = cloudCoverMax }
            }
        };

        var payloadJson = JsonSerializer.Serialize(payload);
        using var resp = await SendWithRetryAsync(
            () => _http.PostAsync(SearchUrl, new StringContent(payloadJson, Encoding.UTF8, "application/json")),
            "STAC search");
        resp.EnsureSuccessStatusCode();

        var searchBody = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(searchBody);

        var features = doc.RootElement.GetProperty("features");
        var items = new List<StacItem>();

        foreach (var feature in features.EnumerateArray())
        {
            items.Add(ParseFeature(feature));
        }

        return items;
    }

    private static string BuildDateTimeInterval(DateTime from, DateTime to)
    {
        var start = from.Date;
        var endExclusive = to.Date.AddDays(1);
        if (endExclusive <= start)
        {
            endExclusive = start.AddDays(1);
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{start:yyyy-MM-dd'T'HH:mm:ss'Z'}/{endExclusive:yyyy-MM-dd'T'HH:mm:ss'Z'}");
    }

    public async Task<StacItem?> GetByIdAsync(string id)
    {
        var payload = new
        {
            collections = new[] { "sentinel-2-l2a" },
            ids = new[] { id },
            limit = 1
        };

        var payloadJson = JsonSerializer.Serialize(payload);
        using var resp = await SendWithRetryAsync(
            () => _http.PostAsync(SearchUrl, new StringContent(payloadJson, Encoding.UTF8, "application/json")),
            "STAC lookup");
        resp.EnsureSuccessStatusCode();

        var body = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(body);

        var features = doc.RootElement.GetProperty("features");
        foreach (var feature in features.EnumerateArray())
        {
            return ParseFeature(feature);
        }

        return null;
    }

    public async Task<string> SignHrefAsync(string href)
    {
        var signEndpoint = "https://planetarycomputer.microsoft.com/api/sas/v1/sign?href=" + Uri.EscapeDataString(href);
        using var resp = await SendWithRetryAsync(() => _http.GetAsync(signEndpoint), "STAC asset signing");
        resp.EnsureSuccessStatusCode();

        var body = await resp.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(body);

        if (json.RootElement.TryGetProperty("href", out var signedHref))
        {
            return signedHref.GetString() ?? throw new Exception("Signer returned null href.");
        }

        throw new Exception("Signer response did not include 'href'. Body: " + body);
    }

    private static async Task<HttpResponseMessage> SendWithRetryAsync(
        Func<Task<HttpResponseMessage>> send,
        string operationName)
    {
        const int maxAttempts = 5;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var resp = await send();
            if (!ShouldRetry(resp.StatusCode) || attempt == maxAttempts)
            {
                return resp;
            }

            var delay = GetRetryDelay(resp, attempt);
            Console.WriteLine($"{operationName}: received {(int)resp.StatusCode} {resp.ReasonPhrase}; waiting {delay.TotalSeconds:0} seconds before retry {attempt + 1}/{maxAttempts}.");
            resp.Dispose();
            await Task.Delay(delay);
        }

        throw new InvalidOperationException($"{operationName} retry loop exited unexpectedly.");
    }

    private static bool ShouldRetry(HttpStatusCode statusCode)
    {
        return statusCode == HttpStatusCode.TooManyRequests
            || statusCode == HttpStatusCode.RequestTimeout
            || statusCode == HttpStatusCode.InternalServerError
            || statusCode == HttpStatusCode.BadGateway
            || statusCode == HttpStatusCode.ServiceUnavailable
            || statusCode == HttpStatusCode.GatewayTimeout;
    }

    private static TimeSpan GetRetryDelay(HttpResponseMessage resp, int attempt)
    {
        var retryAfter = resp.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta && delta > TimeSpan.Zero)
        {
            return ClampDelay(delta);
        }

        if (retryAfter?.Date is { } date)
        {
            var fromHeader = date - DateTimeOffset.UtcNow;
            if (fromHeader > TimeSpan.Zero)
            {
                return ClampDelay(fromHeader);
            }
        }

        var seconds = Math.Min(300, 15 * Math.Pow(2, attempt - 1));
        return TimeSpan.FromSeconds(seconds);
    }

    private static TimeSpan ClampDelay(TimeSpan delay)
    {
        if (delay < TimeSpan.FromSeconds(5))
        {
            return TimeSpan.FromSeconds(5);
        }

        if (delay > TimeSpan.FromMinutes(10))
        {
            return TimeSpan.FromMinutes(10);
        }

        return delay;
    }

    private static StacItem ParseFeature(JsonElement feature)
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

        return new StacItem(id, cloud, assets, feature.GetRawText());
    }
}
