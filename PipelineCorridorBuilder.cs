using System.Globalization;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Buffer;

public sealed class PipelineCorridorBuilder
{
    private const int Wgs84Srid = 4326;
    private readonly GeometryFactory _wgs84Factory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: Wgs84Srid);

    public PipelineSectionCorridor Build(PipelineSection section, double corridorHalfWidthM)
    {
        if (corridorHalfWidthM <= 0d || !double.IsFinite(corridorHalfWidthM))
        {
            throw new InvalidOperationException("Pipeline corridor half width must be a finite value greater than zero.");
        }

        if (section.UtmZone is < 1 or > 60)
        {
            throw new InvalidOperationException("Pipeline section UTM zone must be between 1 and 60.");
        }

        var reader = new WKTReader(NetTopologySuite.NtsGeometryServices.Instance);
        var geometry = reader.Read(section.RouteSectionWkt);
        if (geometry is not LineString lineString || lineString.NumPoints < 2)
        {
            throw new InvalidOperationException("Pipeline section route geometry must be a LineString with at least two coordinates.");
        }

        var projectedSrid = GetUtmSrid(section.UtmZone, section.NorthernHemisphere);
        var projectedFactory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: projectedSrid);
        var projectedCoordinates = lineString.Coordinates
            .Select(coordinate => ProjectToUtm(coordinate, section.UtmZone, section.NorthernHemisphere))
            .Select(point => new Coordinate(point.X, point.Y))
            .ToArray();
        var projectedLine = projectedFactory.CreateLineString(projectedCoordinates);

        var parameters = new BufferParameters
        {
            EndCapStyle = EndCapStyle.Round,
            JoinStyle = JoinStyle.Round,
            QuadrantSegments = 8
        };
        var projectedCorridor = projectedLine.Buffer(corridorHalfWidthM, parameters);
        projectedCorridor.SRID = projectedSrid;

        var wgs84Corridor = TransformProjectedGeometryToWgs84(
            projectedCorridor,
            section.UtmZone,
            section.NorthernHemisphere);
        wgs84Corridor.SRID = Wgs84Srid;

        return new PipelineSectionCorridor
        {
            Section = section,
            ProjectedSrid = projectedSrid,
            CorridorProjectedWkt = new WKTWriter().Write(projectedCorridor),
            CorridorWgs84Wkt = new WKTWriter().Write(wgs84Corridor),
            CorridorWgs84GeoJson = new GeoJsonWriter().Write(wgs84Corridor)
        };
    }

    private Geometry TransformProjectedGeometryToWgs84(Geometry geometry, int zone, bool northernHemisphere)
    {
        if (geometry is Polygon polygon)
        {
            return TransformProjectedPolygonToWgs84(polygon, zone, northernHemisphere);
        }

        if (geometry is MultiPolygon multiPolygon)
        {
            var polygons = new Polygon[multiPolygon.NumGeometries];
            for (var i = 0; i < multiPolygon.NumGeometries; i++)
            {
                polygons[i] = TransformProjectedPolygonToWgs84((Polygon)multiPolygon.GetGeometryN(i), zone, northernHemisphere);
            }

            return _wgs84Factory.CreateMultiPolygon(polygons);
        }

        throw new InvalidOperationException("Pipeline corridor buffer must produce a Polygon or MultiPolygon.");
    }

    private Polygon TransformProjectedPolygonToWgs84(Polygon polygon, int zone, bool northernHemisphere)
    {
        var shell = TransformProjectedRingToWgs84((LinearRing)polygon.ExteriorRing, zone, northernHemisphere);
        var holes = new LinearRing[polygon.NumInteriorRings];
        for (var i = 0; i < polygon.NumInteriorRings; i++)
        {
            holes[i] = TransformProjectedRingToWgs84((LinearRing)polygon.GetInteriorRingN(i), zone, northernHemisphere);
        }

        return _wgs84Factory.CreatePolygon(shell, holes);
    }

    private LinearRing TransformProjectedRingToWgs84(LinearRing ring, int zone, bool northernHemisphere)
    {
        var coordinates = ring.Coordinates
            .Select(coordinate => ProjectFromUtm(coordinate.X, coordinate.Y, zone, northernHemisphere))
            .ToArray();
        return _wgs84Factory.CreateLinearRing(coordinates);
    }

    private static int GetUtmSrid(int zone, bool northernHemisphere)
    {
        return (northernHemisphere ? 32600 : 32700) + zone;
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

    private static Coordinate ProjectFromUtm(double easting, double northing, int zone, bool northernHemisphere)
    {
        const double semiMajor = 6378137d;
        const double inverseFlattening = 298.257223563d;
        const double scale = 0.9996d;
        var flattening = 1d / inverseFlattening;
        var eccentricitySquared = flattening * (2d - flattening);
        var eccentricityPrimeSquared = eccentricitySquared / (1d - eccentricitySquared);
        var e1 = (1d - Math.Sqrt(1d - eccentricitySquared)) / (1d + Math.Sqrt(1d - eccentricitySquared));

        var x = easting - 500000d;
        var y = northernHemisphere ? northing : northing - 10000000d;
        var lonOrigin = (zone - 1) * 6d - 180d + 3d;
        var m = y / scale;
        var mu = m / (semiMajor * (1d - eccentricitySquared / 4d - 3d * eccentricitySquared * eccentricitySquared / 64d - 5d * Math.Pow(eccentricitySquared, 3d) / 256d));
        var phi1 = mu
            + (3d * e1 / 2d - 27d * Math.Pow(e1, 3d) / 32d) * Math.Sin(2d * mu)
            + (21d * e1 * e1 / 16d - 55d * Math.Pow(e1, 4d) / 32d) * Math.Sin(4d * mu)
            + (151d * Math.Pow(e1, 3d) / 96d) * Math.Sin(6d * mu)
            + (1097d * Math.Pow(e1, 4d) / 512d) * Math.Sin(8d * mu);

        var sinPhi1 = Math.Sin(phi1);
        var cosPhi1 = Math.Cos(phi1);
        var tanPhi1 = Math.Tan(phi1);
        var n1 = semiMajor / Math.Sqrt(1d - eccentricitySquared * sinPhi1 * sinPhi1);
        var t1 = tanPhi1 * tanPhi1;
        var c1 = eccentricityPrimeSquared * cosPhi1 * cosPhi1;
        var r1 = semiMajor * (1d - eccentricitySquared) / Math.Pow(1d - eccentricitySquared * sinPhi1 * sinPhi1, 1.5d);
        var d = x / (n1 * scale);

        var lat = phi1 - (n1 * tanPhi1 / r1)
            * (d * d / 2d
                - (5d + 3d * t1 + 10d * c1 - 4d * c1 * c1 - 9d * eccentricityPrimeSquared) * Math.Pow(d, 4d) / 24d
                + (61d + 90d * t1 + 298d * c1 + 45d * t1 * t1 - 252d * eccentricityPrimeSquared - 3d * c1 * c1) * Math.Pow(d, 6d) / 720d);
        var lon = ToRadians(lonOrigin) + (d
            - (1d + 2d * t1 + c1) * Math.Pow(d, 3d) / 6d
            + (5d - 2d * c1 + 28d * t1 - 3d * c1 * c1 + 8d * eccentricityPrimeSquared + 24d * t1 * t1) * Math.Pow(d, 5d) / 120d) / cosPhi1;

        return new Coordinate(
            Math.Round(ToDegrees(lon), 12, MidpointRounding.AwayFromZero),
            Math.Round(ToDegrees(lat), 12, MidpointRounding.AwayFromZero));
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180d;

    private static double ToDegrees(double radians) => radians * 180d / Math.PI;
}
