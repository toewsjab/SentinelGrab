using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;

public sealed record StacItem(
    string Id,
    double? CloudCover,
    DateTimeOffset? AcquiredAt,
    string? DatatakeId,
    string? MgrsTile,
    Dictionary<string, string> Assets,
    string RawJson)
{
    public string AcquisitionKey => !string.IsNullOrWhiteSpace(DatatakeId)
        ? DatatakeId
        : AcquiredAt?.UtcDateTime.ToString("yyyyMMdd'T'HHmmss", CultureInfo.InvariantCulture) ?? Id;
}

public sealed record StacAcquisitionGroup(
    string AcquisitionKey,
    DateOnly AcquisitionDateUtc,
    DateTimeOffset AcquiredAtUtc,
    IReadOnlyList<StacItem> Items)
{
    public IReadOnlyList<string> MgrsTiles { get; init; } = Items
        .Select(item => item.MgrsTile)
        .Where(tile => !string.IsNullOrWhiteSpace(tile))
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .OrderBy(tile => tile, StringComparer.OrdinalIgnoreCase)
        .Select(tile => tile!)
        .ToList();
}

public sealed class StacClient
{
    private readonly HttpClient _http;
    private const string SearchUrl = "https://planetarycomputer.microsoft.com/api/stac/v1/search";

    public StacClient(HttpClient http)
    {
        _http = http;
    }

    public Task<List<StacItem>> SearchAsync(
        Bbox bbox,
        DateTime from,
        DateTime to,
        int cloudCoverMax,
        int limit,
        CancellationToken cancellationToken = default)
    {
        var payload = new Dictionary<string, object?>
        {
            ["collections"] = new[] { "sentinel-2-l2a" },
            ["bbox"] = bbox.ToArray(),
            ["datetime"] = BuildDateTimeInterval(from, to),
            ["limit"] = limit,
            ["query"] = new Dictionary<string, object>
            {
                ["eo:cloud_cover"] = new Dictionary<string, object> { ["lte"] = cloudCoverMax }
            }
        };

        return SearchPagedAsync(payload, limit, "STAC bbox search", cancellationToken);
    }

    public Task<List<StacItem>> SearchIntersectsAsync(
        string wgs84GeoJsonPolygonOrMultiPolygon,
        DateTime from,
        DateTime to,
        int cloudCoverMax,
        int pageLimit,
        CancellationToken cancellationToken = default)
    {
        ValidateIntersectsGeometry(wgs84GeoJsonPolygonOrMultiPolygon);
        var intersects = JsonDocument.Parse(wgs84GeoJsonPolygonOrMultiPolygon).RootElement.Clone();
        var payload = new Dictionary<string, object?>
        {
            ["collections"] = new[] { "sentinel-2-l2a" },
            ["intersects"] = intersects,
            ["datetime"] = BuildDateTimeInterval(from, to),
            ["limit"] = pageLimit,
            ["query"] = new Dictionary<string, object>
            {
                ["eo:cloud_cover"] = new Dictionary<string, object> { ["lte"] = cloudCoverMax }
            }
        };

        return SearchPagedAsync(payload, pageLimit, "STAC intersects search", cancellationToken);
    }

    public static IReadOnlyList<StacAcquisitionGroup> GroupByAcquisition(IEnumerable<StacItem> items)
    {
        return items
            .Where(item => item.AcquiredAt.HasValue)
            .GroupBy(item =>
            {
                var acquiredAtUtc = item.AcquiredAt!.Value.ToUniversalTime();
                return new
                {
                    item.AcquisitionKey,
                    Date = DateOnly.FromDateTime(acquiredAtUtc.UtcDateTime.Date)
                };
            })
            .Select(group =>
            {
                var ordered = group
                    .OrderBy(item => item.MgrsTile, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
                    .ToList();
                var acquiredAtUtc = ordered[0].AcquiredAt!.Value.ToUniversalTime();
                return new StacAcquisitionGroup(group.Key.AcquisitionKey, group.Key.Date, acquiredAtUtc, ordered);
            })
            .OrderBy(group => group.AcquiredAtUtc)
            .ThenBy(group => group.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<StacItem?> GetByIdAsync(string id, CancellationToken cancellationToken = default)
    {
        var payload = new Dictionary<string, object?>
        {
            ["collections"] = new[] { "sentinel-2-l2a" },
            ["ids"] = new[] { id },
            ["limit"] = 1
        };

        var items = await SearchPagedAsync(payload, 1, "STAC lookup", cancellationToken);
        return items.FirstOrDefault();
    }

    public async Task<string> SignHrefAsync(string href, CancellationToken cancellationToken = default)
    {
        var signEndpoint = "https://planetarycomputer.microsoft.com/api/sas/v1/sign?href=" + Uri.EscapeDataString(href);
        using var resp = await SendWithRetryAsync(() => _http.GetAsync(signEndpoint, cancellationToken), "STAC asset signing", cancellationToken);
        resp.EnsureSuccessStatusCode();

        var body = await resp.Content.ReadAsStringAsync(cancellationToken);
        using var json = JsonDocument.Parse(body);

        if (json.RootElement.TryGetProperty("href", out var signedHref))
        {
            return signedHref.GetString() ?? throw new InvalidOperationException("Signer returned null href.");
        }

        throw new InvalidOperationException("Signer response did not include 'href'. Body: " + RedactSensitiveUrls(body));
    }

    public static string RedactSensitiveUrl(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) || string.IsNullOrEmpty(uri.Query))
        {
            return value;
        }

        var builder = new UriBuilder(uri)
        {
            Query = "redacted"
        };
        return builder.Uri.ToString();
    }

    public static string RedactSensitiveUrls(string value)
    {
        return System.Text.RegularExpressions.Regex.Replace(
            value,
            @"https?://[^\s""'<>]+",
            match => RedactSensitiveUrl(match.Value),
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private async Task<List<StacItem>> SearchPagedAsync(
        Dictionary<string, object?> payload,
        int pageLimit,
        string operationName,
        CancellationToken cancellationToken)
    {
        if (pageLimit <= 0)
        {
            throw new InvalidOperationException("STAC page limit must be greater than zero.");
        }

        var items = new Dictionary<string, StacItem>(StringComparer.OrdinalIgnoreCase);
        string? nextToken = null;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!string.IsNullOrWhiteSpace(nextToken))
            {
                payload["token"] = nextToken;
            }
            else
            {
                payload.Remove("token");
            }

            var payloadJson = JsonSerializer.Serialize(payload);
            using var resp = await SendWithRetryAsync(
                () => _http.PostAsync(SearchUrl, new StringContent(payloadJson, Encoding.UTF8, "application/json"), cancellationToken),
                operationName,
                cancellationToken);
            resp.EnsureSuccessStatusCode();

            var searchBody = await resp.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(searchBody);
            if (!doc.RootElement.TryGetProperty("features", out var features))
            {
                throw new InvalidOperationException("STAC response did not include a features array.");
            }

            foreach (var feature in features.EnumerateArray())
            {
                var item = ParseFeature(feature);
                items[item.Id] = item;
            }

            nextToken = ReadNextToken(doc.RootElement);
            if (string.IsNullOrWhiteSpace(nextToken))
            {
                break;
            }
        }

        return items.Values
            .OrderBy(item => item.AcquiredAt ?? DateTimeOffset.MaxValue)
            .ThenBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
            .ToList();
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

    private static void ValidateIntersectsGeometry(string geoJson)
    {
        using var doc = JsonDocument.Parse(geoJson);
        if (!doc.RootElement.TryGetProperty("type", out var typeElement) || typeElement.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException("STAC intersects geometry must be GeoJSON Polygon or MultiPolygon.");
        }

        var type = typeElement.GetString();
        if (!string.Equals(type, "Polygon", StringComparison.Ordinal)
            && !string.Equals(type, "MultiPolygon", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("STAC intersects geometry must be GeoJSON Polygon or MultiPolygon.");
        }
    }

    private static string? ReadNextToken(JsonElement root)
    {
        if (!root.TryGetProperty("links", out var links) || links.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        foreach (var link in links.EnumerateArray())
        {
            if (!link.TryGetProperty("rel", out var relElement)
                || !string.Equals(relElement.GetString(), "next", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (link.TryGetProperty("body", out var body)
                && body.TryGetProperty("token", out var bodyToken)
                && bodyToken.ValueKind == JsonValueKind.String)
            {
                return bodyToken.GetString();
            }

            if (link.TryGetProperty("href", out var hrefElement)
                && Uri.TryCreate(hrefElement.GetString(), UriKind.Absolute, out var href))
            {
                var token = ReadQueryParameter(href.Query, "token");
                if (!string.IsNullOrWhiteSpace(token))
                {
                    return token;
                }
            }
        }

        return null;
    }

    private static string? ReadQueryParameter(string query, string name)
    {
        if (string.IsNullOrEmpty(query))
        {
            return null;
        }

        var trimmed = query[0] == '?' ? query[1..] : query;
        foreach (var part in trimmed.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            var key = separator >= 0 ? part[..separator] : part;
            if (!string.Equals(Uri.UnescapeDataString(key), name, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = separator >= 0 ? part[(separator + 1)..] : "";
            return Uri.UnescapeDataString(value.Replace("+", "%20", StringComparison.Ordinal));
        }

        return null;
    }

    private static async Task<HttpResponseMessage> SendWithRetryAsync(
        Func<Task<HttpResponseMessage>> send,
        string operationName,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 5;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            HttpResponseMessage resp;
            try
            {
                resp = await send();
            }
            catch (HttpRequestException) when (attempt < maxAttempts)
            {
                var delay = GetRetryDelay(null, attempt);
                Console.WriteLine($"{operationName}: transient request failure; waiting {delay.TotalSeconds:0} seconds before retry {attempt + 1}/{maxAttempts}.");
                await Task.Delay(delay, cancellationToken);
                continue;
            }

            if (!ShouldRetry(resp.StatusCode) || attempt == maxAttempts)
            {
                return resp;
            }

            var retryDelay = GetRetryDelay(resp, attempt);
            Console.WriteLine($"{operationName}: received {(int)resp.StatusCode} {resp.ReasonPhrase}; waiting {retryDelay.TotalSeconds:0} seconds before retry {attempt + 1}/{maxAttempts}.");
            resp.Dispose();
            await Task.Delay(retryDelay, cancellationToken);
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

    private static TimeSpan GetRetryDelay(HttpResponseMessage? resp, int attempt)
    {
        var retryAfter = resp?.Headers.RetryAfter;
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
        DateTimeOffset? acquiredAt = null;
        string? datatakeId = null;
        string? mgrsTile = null;

        if (feature.TryGetProperty("properties", out var props))
        {
            if (props.TryGetProperty("eo:cloud_cover", out var cc)
                && cc.ValueKind == JsonValueKind.Number)
            {
                cloud = cc.GetDouble();
            }

            if (props.TryGetProperty("datetime", out var datetimeElement)
                && datetimeElement.ValueKind == JsonValueKind.String
                && DateTimeOffset.TryParse(
                    datetimeElement.GetString(),
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var parsedDateTime))
            {
                acquiredAt = parsedDateTime;
            }

            if (props.TryGetProperty("s2:datatake_id", out var datatakeElement)
                && datatakeElement.ValueKind == JsonValueKind.String)
            {
                datatakeId = datatakeElement.GetString();
            }

            if (props.TryGetProperty("s2:mgrs_tile", out var mgrsElement)
                && mgrsElement.ValueKind == JsonValueKind.String)
            {
                mgrsTile = mgrsElement.GetString();
            }
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

        return new StacItem(id, cloud, acquiredAt, datatakeId, mgrsTile, assets, feature.GetRawText());
    }
}
