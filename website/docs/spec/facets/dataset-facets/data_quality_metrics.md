---
sidebar_position: 2
---

# Data Quality Metrics Facet

This facet allows platforms to display and monitor metrics related to the health of a given dataset.

Example:

```json
{
    ...
    "inputs": {
        "inputFacets": {
            "dataQualityMetrics": {
                "_producer": "https://some.producer.com/version/1.0",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsDatasetFacet.json",
                "rowCount": 123,
                "fileCount": 5,
                "bytes": 35602,
                "lastUpdated": "2025-05-30T08:42:00.001+10:00",
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
The facet specification can be found [here](https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsDatasetFacet.json).