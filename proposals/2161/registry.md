# OpenLineage registry proposal
## Goal
- Allow third parties to register their implementations or custom extensions to make them easy to discover.
- Shorten “Producer” and “schema url” values

## Concept needing a registry:
Producers:

- Custom facet prefix to registry of facet schemas
- Producer uri to full URL of producer doc
  - Facets produced
- Facet URI to full facet schema url
Consumers:
- URL to Documentation of facets understood.
- Facets consumed

Requirements:

- Producers can create and evolve their custom facets without requiring approval from the OpenLineage project.
- Producers and Consumers can update the list of the facets they produce or consume without requiring approval from the OpenLineage project.
- Consumers can independently discover and support custom facets.
- OpenLineage users can easily explore which producers and/or consumers best meet their compatibility needs
- URIs should be short (producer, faceturl)
- A registered name can be both a producer and a consumer.

## Proposal

### Name registration and shorter URIs
Each consumer or producer entity can claim a name, defined in the registry: “{name}”
Each registered entity will provide a documentation URL for its documentation.
The registered name is used to shorten “producer” and “schemaUrl” fields in facets.

### Core facets
As part of the creation of the registry, the core facets under "spec/facets" will be moved to the registry as well under the "core" name. They will follow all the same constraints as all the other facets in the registry. The "core" name is used to shorten the URIs. ex: "ol:core:{FacetName}"

## Acceptance Guidelines
To claim a name, an entity must have either documentation or a test/sample. "Reserving" a name prior to public functionality is discouraged.

Corresponding values to be used:

- Custom facet Prefix = `“${name}”`
- Producer URI prefix = `“ol:${name}”`  can use sub URIs if more than one component needed `“ol:${name}:${sub component}”` or `“ol:${name}:${version}”`
- Schema URI prefix: `“ol:${name}:${path}”` => `“${schema url prefix}/${path}”`

### CI and Documentation

#### CI validation
- The registry is consistent:
  - Validate custom facet prefixes match the registered name
- The registry has the required fields
- Linting.
- Custom facet schemas are valid and validated against examples.

#### The registry will be used to publish documentation in CI
- A page similar to our ecosystem page that lists producers, consumers, links to their documentation and what facets they support.
- Custom facets on their schema URL like the current core facets:
 Ex: https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json
- Generated doc from the json schemas of the facets. Publish them on openlineage.io

#### Additional documentation
We can create a documentation page per use case to document what facet can be use in what case:

- Compliance with privacy laws (GDPR, CCPA, …)
- Compliance with Banking regulation (BCBS-239)
- Data reliability, data quality
- Data discovery, data catalog
- Data Governance
- Data Lineage
- …

We should have a page explaining how a custom facet can be promoted to a core facet through the OpenLineage Proposal process.

### The Registry

We propose a self contained registry hosted in the OpenLineage repository.

**TL;DR: The registry defines consumers and producers names and contains all their custom facets.**

Access control is delegated using `CODEOWNERS` files
Each participant in the ecosystem owns their own folder.

Structure:

```
OpenLineage/spec/registry/
	/{Name}/           <- custom facet schemas are stored in this folder
	                   <- and respect the same rules as spec/facets
		CODEOWNERS     <- Delegate approval to the owners of the name
		registry.json: <- one file per participant
			Producer: 
				Producer root doc URL: https://…
				Produced facets:
					{ facets: [ {“URI}”, “{URI}”, … ]}
			Consumer: 
				Consumer root doc URL: https://…
				Consumed facets:
					{ facets: [ “{URI}”, “{URI}”, … ]}
		/facets/       <- where custom facet schemas are stored
                /facets/examples/{FacetName}/{number}.json       <- where facet examples are stored
```
Facet examples are  currently in [spec/tests](https://github.com/OpenLineage/OpenLineage/blob/main/spec/tests/ColumnLineageDatasetFacet/1.json)

Examples:

In `OpenLineage/spec/registry/`

```
airflow/
	CODEOWNERS
	registry.json
	{
		producer: {
			root_doc_URL: “https://airflow/doc”
			produced_facets: [
				“ol:airflow:AirflowRunFacet.json”,
				“ol:core:1-0-0/DatasetVersionDatasetFacet.json”,
				…
			]
		}
	}
	facets/AirflowRunFacet.json
```
```
core/
	CODEOWNERS
	registry.json
	{
		producer: {
			root_doc_URL: "https://openlineage.io/spec/facets/",
			sample_URL: "https://github.com/OpenLineage/OpenLineage/tree/main/spec/tests/",
			facets: [
				"ColumnLineageDatasetFacet.json": {
					"owner": "core",
					"spec_versions": [ "1-0-1", "1-0-0" ],
					"use_cases": [ "lineage", "catalog" ]
				},
				"DataQualityAssertionsDatasetFacet.json": {
					"owner": "core",
					"spec_versions": [ "1-0-0" ],
					"use_cases": [ "data quality" ]
				}
			]
		}
	}
```
```
egeria/
	CODEOWNERS
	registry.json
	{
		producer: {    
			root_doc_URL: … ,
			sample_URL: … , 
			facets: [
				"ColumnLineageDatasetFacet.json": {
					"owner": "core",
					"spec_versions": [ "1-0-1" ],
					"use_cases": [ "lineage", "catalog" ]
				},
				"NewCustomFacet.json": {
					"owner": "egeria",
					"spec_versions": [ "1-0-0" ],
					"use_cases": [ "lineage" ]
				}
			]
		},
		consumer: {
			root_doc_URL: …
			facets: [ 
				"NewCustomFacet.json": {
					"owner": "egeria",
					"spec_versions": [ "1-0-0" ],
					"use_cases": [ "lineage" ]
				}
			]
		}
	}
```
```
manta/
	CODEOWNERS
	registry.json
	{
		consumer: {
			root_doc_URL: “https://manta.com/doc”
			consumed_facets: [ … ]
		}
	}
```

These files get published on openlineage.io just like the official spec:

- `https://openlineage.io/spec/2-0-1/OpenLineage.json`
- `https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json`

The Airflow producer should now use:

- Custom facet prefix = `airflow`
- Producer URI prefix = `ol:airflow`
- Schema URI prefix: `ol:airflow:`
- Schema URI: `ol:airflow:AirflowRunFacet.json` => `https://github.com/apache/airflow/providers/openlineage/schemas/AirflowRunFacet.json`

Pros:

- Once the name is registered, producers and consumers define codeowners to approve changes to the registry (documentation location, custom facets, …)
- CI can guarantee that the changes to the registry do not make it inconsistent.
- Producers do not need to host and maintain their own subset of the registry. 
- Publication is automated and is consistent with current core spec.

Cons:

- Registered entities must maintain their list of codeowners to guarantee that they can keep updating their own definition.


### Alternative considered
Minimizing what is in the central registry and referring to externally hosted artifacts:

**TL;DR: A central registry that defines only names for producers and consumers but externalizes custom facets and what is consumed/produced**

The OpenLineage repository contains a single `registry.json` file in spec/registry structured as follows:

```
OpenLineage/spec/registry/registry.json:
	“{Name}”
		Producer: 
			Producer root doc URL: https://…
			Schema URL prefix: actual longer URL where schemas are found: https://…
			Produced facets:
				URL to a json doc containing the list of facet schemas produced
				{ facets: [ {“URI}”, “{URI}”, … ]}
		Consumer: 
			Consumer root doc URL: https://…
			Consumed facets:
				URL to a json doc containing the list of facet schemas consumed
				{ facets: [ “{URI}”, “{URI}”, … ]}
```

Example:

`registry.json`

```
{
	airflow: {
		producer: {
		root_doc_URL: “https://airflow/doc”
		schema_URL_prefix: “https://github.com/apache/airflow/providers/openlineage/schemas/”
		produced_facets:
			“https://github.com/apache/airflow/providers/openlineage/produced_facets.json”
		}
	},
	manta: {
		consumer: {
			root_doc_URL: “https://manta.com/doc”
			Consumed_facets: “https://manta.com/consumedFacets.json”
		}
	}
}
```

`https://github.com/apache/airflow/providers/openlineage/produced_facets.json`

```
{
	facets: [
		“ol:airflow:AirflowRunFacet.json”,
		“ol:core:1-0-0/DatasetVersionDatasetFacet.json”,
		…
	]
}
```
Pros:

- Once the name is registered, producers and consumers fully own the lifecycle of their facets without relying on any core repo interaction

Cons:

- Producers need to host and maintain their own subset of the registry
- The core repo ci cannot guarantee consistency of the registry as it relies on external references
- The core repo cannot guarantee accuracy of registry entries (e.g. does an entity actually consume/produce the facets they say they do?)
