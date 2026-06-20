using NetTopologySuite.Geometries;

public static class PipelineGeometryProjector
{
    public static int GetUtmSrid(int zone, bool northernHemisphere)
    {
        if (zone is < 1 or > 60)
        {
            throw new InvalidOperationException("UTM zone must be between 1 and 60.");
        }

        return (northernHemisphere ? 32600 : 32700) + zone;
    }

    public static Coordinate ProjectToUtm(Coordinate coordinate, int zone, bool northernHemisphere)
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

        return new Coordinate(easting, northing);
    }

    public static Coordinate ProjectFromUtm(Coordinate coordinate, int zone, bool northernHemisphere)
    {
        return ProjectFromUtm(coordinate.X, coordinate.Y, zone, northernHemisphere);
    }

    public static Coordinate ProjectFromUtm(double easting, double northing, int zone, bool northernHemisphere)
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

    public static Geometry ProjectWgs84ToUtm(Geometry geometry, int zone, bool northernHemisphere)
    {
        return TransformCoordinates(
            geometry,
            coordinate => ProjectToUtm(coordinate, zone, northernHemisphere),
            GetUtmSrid(zone, northernHemisphere));
    }

    public static Geometry ProjectUtmToWgs84(Geometry geometry, int zone, bool northernHemisphere)
    {
        return TransformCoordinates(
            geometry,
            coordinate => ProjectFromUtm(coordinate, zone, northernHemisphere),
            4326);
    }

    private static Geometry TransformCoordinates(Geometry geometry, Func<Coordinate, Coordinate> transform, int srid)
    {
        var copy = (Geometry)geometry.Copy();
        copy.Apply(new CoordinateSequenceFilter(transform));
        copy.SRID = srid;
        return copy;
    }

    private sealed class CoordinateSequenceFilter : NetTopologySuite.Geometries.ICoordinateSequenceFilter
    {
        private readonly Func<Coordinate, Coordinate> _transform;

        public CoordinateSequenceFilter(Func<Coordinate, Coordinate> transform)
        {
            _transform = transform;
        }

        public bool Done => false;
        public bool GeometryChanged => true;

        public void Filter(CoordinateSequence seq, int i)
        {
            var transformed = _transform(seq.GetCoordinate(i));
            seq.SetX(i, transformed.X);
            seq.SetY(i, transformed.Y);
        }
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180d;

    private static double ToDegrees(double radians) => radians * 180d / Math.PI;
}
