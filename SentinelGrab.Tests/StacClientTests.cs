using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

public sealed class StacClientTests
{
    [Fact]
    public async Task SearchIntersectsFollowsPaginationAndDeduplicatesItems()
    {
        var handler = new QueueHandler(
            FeatureCollection(
                new[] { Feature("item-a", "2026-05-01T16:00:00Z", "take-1", "13UDQ"), Feature("item-a", "2026-05-01T16:00:00Z", "take-1", "13UDQ") },
                nextToken: "next-page"),
            FeatureCollection(
                new[] { Feature("item-b", "2026-05-01T16:01:00Z", "take-1", "13UEQ") },
                nextToken: null));
        var client = new StacClient(new HttpClient(handler));

        var items = await client.SearchIntersectsAsync(
            """
            {"type":"Polygon","coordinates":[[[-103,50],[-102.9,50],[-102.9,50.1],[-103,50.1],[-103,50]]]}
            """,
            new DateTime(2026, 5, 1),
            new DateTime(2026, 5, 1),
            100,
            50);

        Assert.Equal(2, items.Count);
        Assert.Equal(new[] { "item-a", "item-b" }, items.Select(item => item.Id).ToArray());
        Assert.Equal(2, handler.RequestBodies.Count);
        Assert.Contains("\"token\":\"next-page\"", handler.RequestBodies[1]);
    }

    [Fact]
    public void GroupByAcquisitionUsesUtcDateAndTiles()
    {
        var items = new[]
        {
            new StacItem("b", 1, DateTimeOffset.Parse("2026-05-02T00:30:00+02:00"), "take-1", "13UEQ", new(), "{}"),
            new StacItem("a", 1, DateTimeOffset.Parse("2026-05-01T22:15:00Z"), "take-1", "13UDQ", new(), "{}"),
            new StacItem("c", 1, DateTimeOffset.Parse("2026-05-03T00:00:00Z"), "take-2", "13UDQ", new(), "{}")
        };

        var groups = StacClient.GroupByAcquisition(items);

        Assert.Equal(2, groups.Count);
        Assert.Equal("take-1", groups[0].AcquisitionKey);
        Assert.Equal(new DateOnly(2026, 5, 1), groups[0].AcquisitionDateUtc);
        Assert.Equal(new[] { "13UDQ", "13UEQ" }, groups[0].MgrsTiles);
    }

    [Fact]
    public void RedactSensitiveUrlRemovesSignedQuery()
    {
        var redacted = StacClient.RedactSensitiveUrl("https://example.test/cog.tif?sig=secret&se=tomorrow&token=abc");

        Assert.DoesNotContain("secret", redacted);
        Assert.DoesNotContain("token=abc", redacted);
        Assert.Contains("redacted", redacted);
    }

    private static string FeatureCollection(IEnumerable<string> features, string? nextToken)
    {
        var links = nextToken is null
            ? "[]"
            : "[{\"rel\":\"next\",\"href\":\"https://planetarycomputer.microsoft.com/api/stac/v1/search\",\"method\":\"POST\",\"body\":{\"token\":\"" + nextToken + "\"}}]";
        return "{\"type\":\"FeatureCollection\",\"features\":[" + string.Join(",", features) + "],\"links\":" + links + "}";
    }

    private static string Feature(string id, string acquiredAt, string datatakeId, string mgrsTile)
    {
        return "{\"type\":\"Feature\",\"id\":\"" + id + "\",\"properties\":{\"datetime\":\"" + acquiredAt + "\",\"eo:cloud_cover\":12.3,\"s2:datatake_id\":\"" + datatakeId + "\",\"s2:mgrs_tile\":\"" + mgrsTile + "\"},\"assets\":{\"B03\":{\"href\":\"https://example.test/" + id + "/B03.tif\"}}}";
    }

    private sealed class QueueHandler : HttpMessageHandler
    {
        private readonly Queue<string> _responses;

        public QueueHandler(params string[] responses)
        {
            _responses = new Queue<string>(responses);
        }

        public List<string> RequestBodies { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            RequestBodies.Add(await request.Content!.ReadAsStringAsync(cancellationToken));
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_responses.Dequeue(), Encoding.UTF8, "application/json")
            };
        }
    }
}
