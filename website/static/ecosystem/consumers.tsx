export type Partner = Record<
  "image" | "org" | "description" | "docs_url" | "org_url", 
  string
>

export const Consumers: Array<Partner> = [
  {  
    image: "amundsen_logo_1.svg",
    org: "Amundsen",
    description: "The Amundsen integration's OpenLineageTableLineageExtractor extracts table lineage information from OpenLineage events.",
    docs_url: "https://www.amundsen.io/amundsen/databuilder/#openlineagetablelineageextractor",
    org_url: "https://www.amundsen.io"
  },
  {
    image: "astronomer_logo.svg",
    org: "Astronomer",
    description: "Astronomer's Astro uses the openlineage-airflow library to extract lineage from Airflow tasks and stores that data in the Astro control plane. The Astronomer UI then renders a graph and list of all tasks and datasets that include OpenLineage data.",
    docs_url: "https://www.astronomer.io/product/",
    org_url: "https://docs.astronomer.io/astro/data-lineage-concepts"
  },
  {
    image: "atlan_logo.svg",
    org: "Atlan",
    description: "Atlan's OpenLineage integration uses job facets to catalog operational metadata from pipelines, enrich existing assets, and provide persona-based lineage information using OpenLineage SDKs.",
    docs_url: "https://atlan.com/?utm_source=partner&utm_medium=referral&utm_campaign=OpenLineage",
    org_url: "https://atlan.com"
  },
  {
    image: "datahub_logo.svg",
    org: "DataHub",
    description: "DataHub's OpenLineage Converter uses an OpenLineageToDataHub class to translate OpenLineage events into DataHub aspects.",
    docs_url: "https://github.com/datahub-project/datahub/tree/master/metadata-integration/java/openlineage-converter",
    org_url: "https://datahubproject.io/"
  },
  {
    image: "egeria_logo_new.svg",
    org: "Egeria",
    description: "Egeria's OpenLineage integration can capture OpenLineage events directly via HTTP or the proxy backend.",
    docs_url: "https://egeria-project.org/features/lineage-management/overview/#the-openlineage-standard",
    org_url: "https://github.com/odpi/egeria"
  },
  {
    image: "google_logo.svg",
    org: "Google Cloud",
    description: "The Google Cloud Data Catalog supports importing OpenLineage events through the Data Lineage API to display in the Dataplex UI alongside lineage information from Google Cloud services including Dataproc.",
    docs_url: "https://cloud.google.com/data-catalog/docs/how-to/open-lineage",
    org_url: "https://cloud.google.com"
  },
  {
    image: "grai_logo.svg",
    org: "Grai",
    description: "The Grai integration makes OpenLineage metadata from various systems available via an OpenLineage-compatible endpoint and a standalone Python library.",
    docs_url: "https://docs.grai.io/integrations/openlineage",
    org_url: "https://www.grai.io/"
  },
  {
    image: "manta_logo_bkgd.svg",
    org: "Manta",
    description: "Manta's OpenLineage Scanner uses job facets to ingest OpenLineage metadata and enrich overall enterprise data pipeline analysis.",
    docs_url: "",
    org_url: "https://getmanta.com/?utm_source=partner&utm_medium=referral&utm_campaign=OpenLineage"
  },
  {
    image: "mqz_logo_new.svg",
    org: "Marquez",
    description: "Marquez is a metadata server offering an OpenLineage-compatible endpoint for real-time collection of information about running jobs and applications.",
    docs_url: "",
    org_url: "https://marquezproject.ai"
  },
  {
    image: "metaphor_logo.svg",
    org: "Metaphor",
    description: "Metaphor's HTTP endpoint processes OpenLineage events and extracts lineage, data quality metadata, and job facets to enable data governance and data enablement across an organization.",
    docs_url: "",
    org_url: "https://metaphor.io"
  },
  {
    image: "ms_logo.svg",
    org: "Microsoft",
    description: "As Airflow DAGs run, Azure Event Hubs collect OpenLineage events for parsing and ingestion by Microsoft Purview, which also ingests events from Spark operations in Azure Databricks via the Azure Databricks to Purview Lineage Connector.",
    docs_url: "https://learn.microsoft.com/en-us/purview/how-to-lineage-airflow",
    org_url: "https://github.com/microsoft/Purview-ADB-Lineage-Solution-Accelerator/"
  },
  {
    image: "omd-logo.svg",
    org: "OpenMetadata",
    description: "An OpenLineage connector collects OpenLineage events via a KafkaConsumer and transforms them into OpenMetadata Lineage edges.",
    docs_url: "https://github.com/open-metadata/OpenMetadata/pull/15317",
    org_url: "https://open-metadata.org/"
  }, 
  ]
