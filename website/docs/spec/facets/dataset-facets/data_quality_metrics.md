---
sidebar_position: 4
---

# Data Quality Metrics Facet

This facet allows platforms to display and monitor metrics related to the health of a given dataset.

Fields description:
- `rowCount`: The number of rows evaluated.
- `bytes`: The size of the dataset in bytes.
- `fileCount`: The number of files evaluated.
- `lastUpdated`: The last time the dataset was changed.
- `captureDate`: An [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) timestamp of when these metrics were captured. All metrics in this facet reflect the dataset state as of this time, which lets consumers compare metrics captured at exactly the same moment (e.g. for data-diff).
- `columnMetrics`: Per-column metrics, keyed by column name.

Example:

```json
{
    ...
    "inputs": {
        "facets": {
            "dataQualityMetrics": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/DataQualityMetricsDatasetFacet.json",
                "rowCount": 123,
                "fileCount": 5,
                "bytes": 35602,
                "lastUpdated": "2025-05-30T08:42:00.001+10:00",
                "captureDate": "2025-05-30T11:00:00Z",
                "columnMetrics": {
                    "column_one": {
                        "nullCount": 132,
                        "distincCount": 11,
                        "sum": 500,
                        "count": 234,
                        "min": 111,
                        "max": 3234,
                        "quantiles": {
                            "0.1": 12,
                            "0.5": 22,
                            "1": 123,
                            "2": 11
                        }
                    },
                    "column_two": {
                        "nullCount": 132,
                        "distinctCount": 11,
                        "sum": 500,
                        "count": 234,
                        "min": 111,
                        "max": 3234,
                        "quantiles": {
                            "0.1": 12,
                            "0.5": 22,
                            "1": 123,
                            "2": 11
                        }
                    },
                    "column_three": {
                        "nullCount": 132,
                        "distincCount": 11,
                        "sum": 500,
                        "count": 234,
                        "min": 111,
                        "max": 3234,
                        "quantiles": {
                            "0.1": 12,
                            "0.5": 22,
                            "1": 123,
                            "2": 11
                        }
                    }
                }
            }
        }
    }
    ...
}
```
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-1/DataQualityMetricsDatasetFacet.json).