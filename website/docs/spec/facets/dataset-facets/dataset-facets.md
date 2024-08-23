---
sidebar_position: 1
---

# Dataset Facets

Dataset Facets are generally consisted of common facet that is used both in `inputs` and `outputs` of the OpenLineage event. There are facets that exist specifically for input or output datasets.

```json
{
  ...
  "inputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes-in",
    "facets": {
        # This is where the common dataset facets are located
    },
    "inputFacets": {
        # This is where the input dataset facets are located
    }
  }],  
  "outputs": [{
    "namespace": "postgres://workshop-db:None",
    "name": "workshop.public.taxes-out",
    "facets": {
        # This is where the common dataset facets are located
    },
    "outputFacets": {
        # This is where the output dataset facets are located
    }
  }],
  ...
}
```

In the above Example, Notice that there is a distinction of facets that are common for both input and output dataset, and input or output specific datasets. As for the common datasets, they all reside under the `facets` property. However, input or output specific facets are located either in `inputFacets` or `outputFacets` property.
