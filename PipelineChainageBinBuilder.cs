using System.Globalization;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.LinearReferencing;
using NetTopologySuite.Operation.Buffer;
using NetTopologySuite.Operation.Union;

public sealed class PipelineChainageBinBuilder
{
    private const int Wgs84Srid = 4326;
    private readonly GeometryFactory _wgs84Factory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: Wgs84Srid);
    private readonly WKTReader _wktReader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly WKTWriter _wktWriter = new();

    public IReadOnlyList<PipelineChainageBinGeometry> Build(
        IReadOnlyList<PipelineSection> sections,
        double analysisBinLengthM,
        double corridorHalfWidthM)
    {
        if (sections.Count == 0)
        {
            return Array.Empty<PipelineChainageBinGeometry>();
        }

        if (!double.IsFinite(analysisBinLengthM) || analysisBinLengthM <= 0d)
        {
            throw new InvalidOperationException("Pipeline bin length must be a finite value greater than zero.");
        }

        if (!double.IsFinite(corridorHalfWidthM) || corridorHalfWidthM <= 0d)
        {
            throw new InvalidOperationException("Pipeline corridor half width must be a finite value greater than zero.");
        }

        var orderedSections = sections
            .OrderBy(section => section.StartChainageM)
            .ThenBy(section => section.SectionOrdinal)
            .ToList();
        ValidateContinuousSections(orderedSections);

        var routeStart = (double)orderedSections[0].StartChainageM;
        var routeEnd = (double)orderedSections[^1].EndChainageM;
        var bins = new List<PipelineChainageBinGeometry>();
        var binIndex = 0;

        for (var binStart = routeStart; binStart < routeEnd - 0.0005d; binStart += analysisBinLengthM)
        {
            var binEnd = Math.Min(routeEnd, binStart + analysisBinLengthM);
            var parts = new List<PipelineChainageBinPart>();

            foreach (var section in orderedSections)
            {
                var sectionStart = (double)section.StartChainageM;
                var sectionEnd = (double)section.EndChainageM;
                var partStart = Math.Max(binStart, sectionStart);
                var partEnd = Math.Min(binEnd, sectionEnd);
                if (partEnd <= partStart + 0.0005d)
                {
                    continue;
                }

                parts.Add(BuildPart(section, binIndex, binStart, binEnd, partStart, partEnd, corridorHalfWidthM));
            }

            if (parts.Count == 0)
            {
                throw new InvalidOperationException(
                    $"Pipeline chainage bin {binIndex.ToString(CultureInfo.InvariantCulture)} did not intersect any processing section.");
            }

            bins.Add(new PipelineChainageBinGeometry
            {
                Bin = new PipelineChainageBin
                {
                    BinIndex = binIndex,
                    SectionOrdinal = parts[0].Bin.SectionOrdinal,
                    StartChainageM = ToDecimalMetres(binStart),
                    EndChainageM = ToDecimalMetres(binEnd),
                    RouteBinWkt = BuildRouteBinWgs84Wkt(parts)
                },
                Parts = parts,
                RouteBinWgs84Wkt = BuildRouteBinWgs84Wkt(parts)
            });

            binIndex++;
        }

        return bins;
    }

    private PipelineChainageBinPart BuildPart(
        PipelineSection section,
        int binIndex,
        double binStart,
        double binEnd,
        double partStart,
        double partEnd,
        double corridorHalfWidthM)
    {
        var routeGeometry = _wktReader.Read(section.RouteSectionWkt);
        if (routeGeometry is not LineString routeLineWgs84 || routeLineWgs84.NumPoints < 2)
        {
            throw new InvalidOperationException("Pipeline section route geometry must be a LineString with at least two coordinates.");
        }

        var projectedSrid = PipelineGeometryProjector.GetUtmSrid(section.UtmZone, section.NorthernHemisphere);
        var routeLineProjected = (LineString)PipelineGeometryProjector.ProjectWgs84ToUtm(routeLineWgs84, section.UtmZone, section.NorthernHemisphere);
        var sectionLength = Math.Max(0d, (double)section.EndChainageM - (double)section.StartChainageM);
        var projectedLength = routeLineProjected.Length;
        var startIndex = ChainageToProjectedIndex(partStart, section, sectionLength, projectedLength);
        var endIndex = ChainageToProjectedIndex(partEnd, section, sectionLength, projectedLength);
        if (endIndex <= startIndex)
        {
            endIndex = Math.Min(projectedLength, startIndex + 0.001d);
        }

        var indexedLine = new LengthIndexedLine(routeLineProjected);
        var routePartProjected = indexedLine.ExtractLine(startIndex, endIndex);
        routePartProjected.SRID = projectedSrid;

        var corridorPartProjected = routePartProjected.Buffer(corridorHalfWidthM, new BufferParameters
        {
            EndCapStyle = EndCapStyle.Round,
            JoinStyle = JoinStyle.Round,
            QuadrantSegments = 8
        });
        corridorPartProjected.SRID = projectedSrid;

        var routePartWgs84 = PipelineGeometryProjector.ProjectUtmToWgs84(routePartProjected, section.UtmZone, section.NorthernHemisphere);
        var corridorPartWgs84 = PipelineGeometryProjector.ProjectUtmToWgs84(corridorPartProjected, section.UtmZone, section.NorthernHemisphere);

        return new PipelineChainageBinPart
        {
            Bin = new PipelineChainageBin
            {
                BinIndex = binIndex,
                SectionOrdinal = section.SectionOrdinal,
                StartChainageM = ToDecimalMetres(partStart),
                EndChainageM = ToDecimalMetres(partEnd),
                RouteBinWkt = _wktWriter.Write(routePartWgs84)
            },
            ProjectedSrid = projectedSrid,
            RouteBinProjectedWkt = _wktWriter.Write(routePartProjected),
            RouteBinWgs84Wkt = _wktWriter.Write(routePartWgs84),
            CorridorProjectedWkt = _wktWriter.Write(corridorPartProjected),
            CorridorWgs84Wkt = _wktWriter.Write(corridorPartWgs84)
        };
    }

    private static double ChainageToProjectedIndex(
        double chainageM,
        PipelineSection section,
        double sectionLength,
        double projectedLength)
    {
        if (sectionLength <= 0d || projectedLength <= 0d)
        {
            return 0d;
        }

        var fraction = (chainageM - (double)section.StartChainageM) / sectionLength;
        return Math.Clamp(fraction * projectedLength, 0d, projectedLength);
    }

    private string BuildRouteBinWgs84Wkt(IReadOnlyCollection<PipelineChainageBinPart> parts)
    {
        if (parts.Count == 1)
        {
            return parts.First().RouteBinWgs84Wkt;
        }

        var lines = parts
            .Select(part => _wktReader.Read(part.RouteBinWgs84Wkt))
            .OfType<LineString>()
            .ToArray();
        return _wktWriter.Write(_wgs84Factory.CreateMultiLineString(lines));
    }

    private static void ValidateContinuousSections(IReadOnlyList<PipelineSection> sections)
    {
        for (var i = 1; i < sections.Count; i++)
        {
            if (sections[i].StartChainageM != sections[i - 1].EndChainageM)
            {
                throw new InvalidOperationException("Pipeline sections must preserve continuous global chainage.");
            }
        }
    }

    private static decimal ToDecimalMetres(double value) => Math.Round((decimal)value, 3, MidpointRounding.AwayFromZero);
}
