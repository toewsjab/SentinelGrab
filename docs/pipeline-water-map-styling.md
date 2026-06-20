# Pipeline Water Map Styling

`PIPELINE_WATER` outputs describe observed surface-water exposure along an ordered route. They are a prioritization/screening layer, not a direct measurement of coating condition, groundwater at pipe depth, cathodic protection effectiveness or external corrosion.

Generic GeoJSON styling:

- `Persistent`: darkest line
- `Seasonal`: medium line
- `Intermittent`: light line
- `InsufficientData`: dashed/hatched
- `Crossing`: separate point/segment symbol

Do not calculate a corrosion-risk score from these exports unless separately supplied with validated inputs such as coating, CP, soil resistivity, drainage, ILI/CIS/DCVG/ACVG or inspection data.
