using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

public sealed class PipelinePathImporter
{
    private const int Wgs84Srid = 4326;
    private const double EarthRadiusM = 6371008.8d;
    private readonly GeometryFactory _geometryFactory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: Wgs84Srid);

    public PipelinePathImportResult Import(PipelinePathImportRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.SourceText))
        {
            throw new InvalidOperationException("Pipeline path source is empty.");
        }

        if (string.IsNullOrWhiteSpace(request.PathName))
        {
            throw new InvalidOperationException("Pipeline path name is required.");
        }

        if (request.EndpointToleranceM < 0)
        {
            throw new InvalidOperationException("Endpoint tolerance cannot be negative.");
        }

        if (request.DensifyMaxSegmentLengthM <= 0)
        {
            throw new InvalidOperationException("Densify maximum segment length must be greater than zero.");
        }

        if (request.MaxProjectedSectionLengthM <= 0)
        {
            throw new InvalidOperationException("Maximum projected section length must be greater than zero.");
        }

        var geometry = ReadGeometry(request.SourceText);
        var originalCoordinates = ExtractOrderedCoordinates(geometry, request.EndpointToleranceM, out var endpointGaps);
        ValidateCoordinates(originalCoordinates);
        RejectBranchingPath(originalCoordinates);

        var densified = Densify(originalCoordinates, request.DensifyMaxSegmentLengthM);
        var sectionPoints = InsertUtmBoundaryPoints(densified);
        var sections = BuildSections(sectionPoints, request.MaxProjectedSectionLengthM, request.ChainageOriginM, out var routeLengthM);
        if (sections.Count == 0)
        {
            throw new InvalidOperationException("Pipeline path did not produce any processing sections.");
        }

        var normalizedRoute = _geometryFactory.CreateLineString(originalCoordinates.ToArray());
        normalizedRoute.SRID = Wgs84Srid;
        var normalizedWkt = new WKTWriter().Write(normalizedRoute);
        var sourceHash = ComputeSourceHash(originalCoordinates, request.ChainageOriginM);

        var first = originalCoordinates[0];
        var last = originalCoordinates[^1];
        var crossedZones = sections
            .Select(section => section.UtmZone)
            .Distinct()
            .OrderBy(zone => zone)
            .ToList();

        return new PipelinePathImportResult
        {
            Path = new SentinelPipelinePathRecord
            {
                PathName = request.PathName,
                RouteGeometry = normalizedWkt,
                RouteLengthM = ToDecimalMetres(routeLengthM),
                ChainageOriginM = request.ChainageOriginM,
                DirectionDescription = request.DirectionDescription,
                SourceReference = request.SourceReference,
                SourceHash = sourceHash,
                IsActive = true
            },
            Sections = sections,
            EndpointGaps = endpointGaps,
            StartLongitude = first.X,
            StartLatitude = first.Y,
            EndLongitude = last.X,
            EndLatitude = last.Y,
            CrossedUtmZones = crossedZones
        };
    }

    private Geometry ReadGeometry(string sourceText)
    {
        var text = sourceText.Trim();
        if (text.StartsWith("{", StringComparison.Ordinal))
        {
            return ReadGeoJsonGeometry(text);
        }

        var reader = new WKTReader(NetTopologySuite.NtsGeometryServices.Instance);
        var geometry = reader.Read(text);
        geometry.SRID = Wgs84Srid;
        return geometry;
    }

    private Geometry ReadGeoJsonGeometry(string text)
    {
        using var document = JsonDocument.Parse(text);
        var root = document.RootElement;
        if (!root.TryGetProperty("type", out var typeElement))
        {
            throw new InvalidOperationException("GeoJSON input is missing a type property.");
        }

        if (root.TryGetProperty("crs", out var crsElement)
            && !crsElement.GetRawText().Contains("4326", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Pipeline path GeoJSON must be EPSG:4326 longitude/latitude coordinates.");
        }

        var type = typeElement.GetString();
        JsonElement geometryElement;
        if (string.Equals(type, "FeatureCollection", StringComparison.Ordinal))
        {
            if (!root.TryGetProperty("features", out var featuresElement) || featuresElement.GetArrayLength() != 1)
            {
                throw new InvalidOperationException("GeoJSON FeatureCollection must contain exactly one feature.");
            }

            geometryElement = featuresElement[0].GetProperty("geometry");
        }
        else if (string.Equals(type, "Feature", StringComparison.Ordinal))
        {
            geometryElement = root.GetProperty("geometry");
        }
        else
        {
            geometryElement = root;
        }

        var reader = new GeoJsonReader();
        var geometry = reader.Read<Geometry>(geometryElement.GetRawText());
        geometry.SRID = Wgs84Srid;
        return geometry;
    }

    private static List<Coordinate> ExtractOrderedCoordinates(
        Geometry geometry,
        double endpointToleranceM,
        out List<PipelinePathEndpointGap> endpointGaps)
    {
        endpointGaps = new List<PipelinePathEndpointGap>();
        if (geometry.IsEmpty)
        {
            throw new InvalidOperationException("Pipeline path geometry is empty.");
        }

        if (!geometry.IsValid)
        {
            throw new InvalidOperationException("Pipeline path geometry is invalid.");
        }

        if (geometry is LineString lineString)
        {
            if (lineString.NumPoints < 2)
            {
                throw new InvalidOperationException("Pipeline path LineString must contain at least two coordinates.");
            }

            return lineString.Coordinates.ToList();
        }

        if (geometry is MultiLineString multiLineString)
        {
            if (multiLineString.NumGeometries == 0)
            {
                throw new InvalidOperationException("Pipeline path MultiLineString is empty.");
            }

            var ordered = new List<Coordinate>();
            for (var i = 0; i < multiLineString.NumGeometries; i++)
            {
                if (multiLineString.GetGeometryN(i) is not LineString component || component.NumPoints < 2)
                {
                    throw new InvalidOperationException("Pipeline path MultiLineString components must be LineStrings with at least two coordinates.");
                }

                var coordinates = component.Coordinates;
                if (i > 0)
                {
                    var previousEnd = ordered[^1];
                    var nextStart = coordinates[0];
                    var gapM = HaversineDistance(previousEnd, nextStart);
                    endpointGaps.Add(new PipelinePathEndpointGap
                    {
                        FromComponentIndex = i - 1,
                        ToComponentIndex = i,
                        GapMetres = gapM
                    });

                    if (gapM > endpointToleranceM)
                    {
                        throw new InvalidOperationException(
                            $"Pipeline path MultiLineString component {i} does not continue from component {i - 1}; endpoint gap is {gapM:0.###} m.");
                    }
                }

                ordered.AddRange(i == 0 ? coordinates : coordinates.Skip(1));
            }

            return ordered;
        }

        throw new InvalidOperationException("Pipeline path input must be a LineString or non-branching MultiLineString.");
    }

    private static void ValidateCoordinates(IReadOnlyList<Coordinate> coordinates)
    {
        for (var i = 0; i < coordinates.Count; i++)
        {
            var coordinate = coordinates[i];
            if (!double.IsFinite(coordinate.X) || coordinate.X < -180d || coordinate.X > 180d)
            {
                throw new InvalidOperationException($"Pipeline path coordinate {i} has invalid longitude {coordinate.X.ToString("G17", CultureInfo.InvariantCulture)}.");
            }

            if (!double.IsFinite(coordinate.Y) || coordinate.Y < -90d || coordinate.Y > 90d)
            {
                throw new InvalidOperationException($"Pipeline path coordinate {i} has invalid latitude {coordinate.Y.ToString("G17", CultureInfo.InvariantCulture)}.");
            }

            if (coordinate.Y < -80d || coordinate.Y > 84d)
            {
                throw new InvalidOperationException($"Pipeline path coordinate {i} latitude is outside the UTM projection range.");
            }

            if (i > 0 && Math.Abs(coordinate.X - coordinates[i - 1].X) > 180d)
            {
                throw new InvalidOperationException("Pipeline path crosses the antimeridian, which is not supported for UTM-section chainage.");
            }
        }
    }

    private static void RejectBranchingPath(IReadOnlyList<Coordinate> coordinates)
    {
        var factory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: Wgs84Srid);
        var route = factory.CreateLineString(coordinates.ToArray());
        if (!route.IsSimple)
        {
            throw new InvalidOperationException("Pipeline path must be a single non-branching route; self-intersections or branch nodes were found.");
        }
    }

    private static List<Coordinate> Densify(IReadOnlyList<Coordinate> coordinates, double maxSegmentLengthM)
    {
        var densified = new List<Coordinate> { Copy(coordinates[0]) };
        for (var i = 1; i < coordinates.Count; i++)
        {
            var start = coordinates[i - 1];
            var end = coordinates[i];
            var distanceM = HaversineDistance(start, end);
            var parts = Math.Max(1, (int)Math.Ceiling(distanceM / maxSegmentLengthM));
            for (var part = 1; part <= parts; part++)
            {
                densified.Add(InterpolateGreatCircle(start, end, (double)part / parts));
            }
        }

        return densified;
    }

    private static List<Coordinate> InsertUtmBoundaryPoints(IReadOnlyList<Coordinate> coordinates)
    {
        var output = new List<Coordinate> { Copy(coordinates[0]) };
        for (var i = 1; i < coordinates.Count; i++)
        {
            var start = output[^1];
            var end = coordinates[i];
            var startZone = GetUtmZone(start.X);
            var endZone = GetUtmZone(end.X);
            if (startZone != endZone)
            {
                var step = Math.Sign(endZone - startZone);
                var current = start;
                for (var zone = startZone; zone != endZone; zone += step)
                {
                    var boundaryLon = step > 0
                        ? -180d + (zone * 6d)
                        : -180d + ((zone - 1) * 6d);
                    var fraction = (boundaryLon - current.X) / (end.X - current.X);
                    if (fraction > 0d && fraction < 1d)
                    {
                        var boundary = InterpolateGreatCircle(current, end, fraction);
                        boundary.X = boundaryLon;
                        output.Add(boundary);
                        current = boundary;
                    }
                }
            }

            output.Add(Copy(end));
        }

        return output;
    }

    private List<PipelinePathSectionRecord> BuildSections(
        IReadOnlyList<Coordinate> coordinates,
        double maxProjectedSectionLengthM,
        decimal chainageOriginM,
        out double routeLengthM)
    {
        var sections = new List<PipelinePathSectionRecord>();
        var sectionPoints = new List<Coordinate> { Copy(coordinates[0]) };
        var sectionStartChainage = (double)chainageOriginM;
        var globalChainage = (double)chainageOriginM;
        var sectionLength = 0d;
        var sectionZone = GetUtmZone(coordinates[0].X);
        var sectionNorth = coordinates[0].Y >= 0d;

        for (var i = 1; i < coordinates.Count; i++)
        {
            var segmentStart = sectionPoints[^1];
            var segmentEnd = coordinates[i];
            var endZone = GetUtmZone(segmentEnd.X);
            var endNorth = segmentEnd.Y >= 0d;

            if (endZone != sectionZone || endNorth != sectionNorth)
            {
                CloseSection();
                sectionPoints = new List<Coordinate> { Copy(segmentStart) };
                sectionStartChainage = globalChainage;
                sectionLength = 0d;
                sectionZone = endZone;
                sectionNorth = endNorth;
            }

            var remainingEnd = segmentEnd;
            while (true)
            {
                var segmentLength = ProjectedDistance(segmentStart, remainingEnd, sectionZone, sectionNorth);
                var remainingCapacity = maxProjectedSectionLengthM - sectionLength;
                if (sectionLength > 0d && segmentLength > remainingCapacity)
                {
                    var fraction = remainingCapacity / segmentLength;
                    var cut = InterpolateGreatCircle(segmentStart, remainingEnd, fraction);
                    sectionPoints.Add(cut);
                    sectionLength += remainingCapacity;
                    globalChainage += remainingCapacity;
                    CloseSection();

                    sectionPoints = new List<Coordinate> { Copy(cut) };
                    sectionStartChainage = globalChainage;
                    sectionLength = 0d;
                    segmentStart = cut;
                    continue;
                }

                sectionPoints.Add(Copy(remainingEnd));
                sectionLength += segmentLength;
                globalChainage += segmentLength;
                break;
            }
        }

        CloseSection();
        routeLengthM = globalChainage - (double)chainageOriginM;
        return sections;

        void CloseSection()
        {
            if (sectionPoints.Count < 2 || sectionLength <= 0d)
            {
                return;
            }

            var geometry = _geometryFactory.CreateLineString(sectionPoints.ToArray());
            geometry.SRID = Wgs84Srid;
            sections.Add(new PipelinePathSectionRecord
            {
                SectionOrdinal = sections.Count + 1,
                UtmZone = sectionZone,
                NorthernHemisphere = sectionNorth,
                StartChainageM = ToDecimalMetres(sectionStartChainage),
                EndChainageM = ToDecimalMetres(sectionStartChainage + sectionLength),
                SectionGeometryWkt = new WKTWriter().Write(geometry)
            });
        }
    }

    private static string ComputeSourceHash(IReadOnlyList<Coordinate> coordinates, decimal chainageOriginM)
    {
        var builder = new StringBuilder();
        builder.Append("chainageOriginM=");
        builder.Append(chainageOriginM.ToString("0.###", CultureInfo.InvariantCulture));
        builder.Append('|');
        foreach (var coordinate in coordinates)
        {
            builder.Append(coordinate.X.ToString("0.########", CultureInfo.InvariantCulture));
            builder.Append(',');
            builder.Append(coordinate.Y.ToString("0.########", CultureInfo.InvariantCulture));
            builder.Append(';');
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static Coordinate InterpolateGreatCircle(Coordinate start, Coordinate end, double fraction)
    {
        if (fraction <= 0d)
        {
            return Copy(start);
        }

        if (fraction >= 1d)
        {
            return Copy(end);
        }

        var lat1 = ToRadians(start.Y);
        var lon1 = ToRadians(start.X);
        var lat2 = ToRadians(end.Y);
        var lon2 = ToRadians(end.X);
        var angularDistance = 2d * Math.Asin(Math.Sqrt(
            Math.Pow(Math.Sin((lat2 - lat1) / 2d), 2d)
            + Math.Cos(lat1) * Math.Cos(lat2) * Math.Pow(Math.Sin((lon2 - lon1) / 2d), 2d)));

        if (angularDistance == 0d)
        {
            return Copy(start);
        }

        var a = Math.Sin((1d - fraction) * angularDistance) / Math.Sin(angularDistance);
        var b = Math.Sin(fraction * angularDistance) / Math.Sin(angularDistance);
        var x = a * Math.Cos(lat1) * Math.Cos(lon1) + b * Math.Cos(lat2) * Math.Cos(lon2);
        var y = a * Math.Cos(lat1) * Math.Sin(lon1) + b * Math.Cos(lat2) * Math.Sin(lon2);
        var z = a * Math.Sin(lat1) + b * Math.Sin(lat2);
        var lat = Math.Atan2(z, Math.Sqrt(x * x + y * y));
        var lon = Math.Atan2(y, x);
        return new Coordinate(ToDegrees(lon), ToDegrees(lat));
    }

    private static double HaversineDistance(Coordinate start, Coordinate end)
    {
        var lat1 = ToRadians(start.Y);
        var lat2 = ToRadians(end.Y);
        var deltaLat = lat2 - lat1;
        var deltaLon = ToRadians(end.X - start.X);
        var a = Math.Pow(Math.Sin(deltaLat / 2d), 2d)
            + Math.Cos(lat1) * Math.Cos(lat2) * Math.Pow(Math.Sin(deltaLon / 2d), 2d);
        return EarthRadiusM * 2d * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1d - a));
    }

    private static double ProjectedDistance(Coordinate start, Coordinate end, int zone, bool northernHemisphere)
    {
        var a = ProjectToUtm(start, zone, northernHemisphere);
        var b = ProjectToUtm(end, zone, northernHemisphere);
        var dx = b.X - a.X;
        var dy = b.Y - a.Y;
        return Math.Sqrt(dx * dx + dy * dy);
    }

    private static (double X, double Y) ProjectToUtm(Coordinate coordinate, int zone, bool northernHemisphere)
    {
        const double semiMajor = 6378137d;
        const double inverseFlattening = 298.257223563d;
        const double scale = 0.9996d;
        var flattening = 1d / inverseFlattening;
        var eccentricitySquared = flattening * (2d - flattening);
        var secondEccentricitySquared = eccentricitySquared / (1d - eccentricitySquared);
        var lat = ToRadians(coordinate.Y);
        var lon = ToRadians(coordinate.X);
        var lonOrigin = ToRadians((zone - 1) * 6d - 180d + 3d);
        var sinLat = Math.Sin(lat);
        var cosLat = Math.Cos(lat);
        var tanLat = Math.Tan(lat);
        var n = semiMajor / Math.Sqrt(1d - eccentricitySquared * sinLat * sinLat);
        var t = tanLat * tanLat;
        var c = secondEccentricitySquared * cosLat * cosLat;
        var a = cosLat * (lon - lonOrigin);
        var m = semiMajor
            * ((1d - eccentricitySquared / 4d - 3d * eccentricitySquared * eccentricitySquared / 64d - 5d * Math.Pow(eccentricitySquared, 3d) / 256d) * lat
                - (3d * eccentricitySquared / 8d + 3d * eccentricitySquared * eccentricitySquared / 32d + 45d * Math.Pow(eccentricitySquared, 3d) / 1024d) * Math.Sin(2d * lat)
                + (15d * eccentricitySquared * eccentricitySquared / 256d + 45d * Math.Pow(eccentricitySquared, 3d) / 1024d) * Math.Sin(4d * lat)
                - (35d * Math.Pow(eccentricitySquared, 3d) / 3072d) * Math.Sin(6d * lat));
        var easting = scale * n * (a
            + (1d - t + c) * Math.Pow(a, 3d) / 6d
            + (5d - 18d * t + t * t + 72d * c - 58d * secondEccentricitySquared) * Math.Pow(a, 5d) / 120d)
            + 500000d;
        var northing = scale * (m + n * tanLat * (a * a / 2d
            + (5d - t + 9d * c + 4d * c * c) * Math.Pow(a, 4d) / 24d
            + (61d - 58d * t + t * t + 600d * c - 330d * secondEccentricitySquared) * Math.Pow(a, 6d) / 720d));

        if (!northernHemisphere)
        {
            northing += 10000000d;
        }

        return (easting, northing);
    }

    private static int GetUtmZone(double longitude)
    {
        if (longitude >= 180d)
        {
            return 60;
        }

        return Math.Clamp((int)Math.Floor((longitude + 180d) / 6d) + 1, 1, 60);
    }

    private static Coordinate Copy(Coordinate coordinate) => new(coordinate.X, coordinate.Y);

    private static decimal ToDecimalMetres(double value) => Math.Round((decimal)value, 3, MidpointRounding.AwayFromZero);

    private static double ToRadians(double degrees) => degrees * Math.PI / 180d;

    private static double ToDegrees(double radians) => radians * 180d / Math.PI;
}
