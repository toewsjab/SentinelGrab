public static class PipelineWaterDetectorFactory
{
    public static PipelineWaterSectionDetector Create()
    {
        return (_, _) => throw new InvalidOperationException(
            "PIPELINE_WATER requires the existing WaterPolygonBuilder section detector to be available. " +
            "The committed project does not currently contain WaterPolygonBuilder, so no pipeline-water result was produced.");
    }
}
