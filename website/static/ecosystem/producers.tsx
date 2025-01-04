import { Partner } from "@site/static/ecosystem/consumers";

export const Producers: Array<Partner> = [
    {
        image: "airflow_logo_bkgd_2.svg",
        org: "Airflow",
        full_name: "Airflow",
        description: "A library integrates DAGs for automatic metadata collection.",
        docs_url: "https://github.com/OpenLineage/OpenLineage/blob/main/integration/airflow",
        org_url: "https://airflow.apache.org"
    },
    {
        image: "dagster_logo_lg.svg",
        org: "Dagster",
        full_name: "Dagster",
        description: "A library converts Dagster events to OpenLineage events and emits them to an OpenLineage backend.",
        docs_url: "",
        org_url: "https://dagster.io"
    },
    {
        image: "dbt_logo_bkgd_2.svg",
        org: "dbt",
        full_name: "dbt",
        description: "A wrapper script uses the OpenLineage client for automatic collection of metadata from dbt.",
        docs_url: "",
        org_url: "https://www.getdbt.com"
    },
    {
        image: "egeria_logo_new.svg",
        org: "Egeria",
        full_name: "Egeria",
        description: "Egeria's OpenLineage integration publishes events to lineage integration connectors with OpenLineage listeners registered in the same instance of the Lineage Integrator OMIS.",
        docs_url: "https://egeria-project.org/features/lineage-management/overview/#the-openlineage-standard",
        org_url: "https://github.com/odpi/egeria"
    },
    {
        image: "flink_logo.svg",
        org: "Flink",
        full_name: "Flink",
        description: "The OpenLineage Flink Agent uses jvm instrumentation to emit OpenLineage metadata.",
        docs_url: "",
        org_url: "https://flink.apache.org"
    },
    {
        image: "google_logo.svg",
        org: "Google Cloud",
        full_name: "Google Cloud",
        description: "Dataproc captures lineage events from Spark jobs and publishes them to the Dataplex Data Lineage API, which also accepts OpenLineage events using the ProcessOpenLineageRunEvent REST API method.",
        docs_url: "https://cloud.google.com/data-catalog/docs/reference/data-lineage/rest",
        org_url: "https://cloud.google.com/dataproc/docs/guides/lineage"
    },
    {
        image: "GE_logo.svg",
        org: "Great Expectations",
        full_name: "Great Expectations",
        description: "The OpenLineageValidationAction collects dataset metadata from the Great Expectations ValidationAction.",
        docs_url: "",
        org_url: "https://greatexpectations.io"
    },
    {
        image: "hamilton_logo.png",
        org: "DAGWorks Inc.",
        full_name: "Hamilton",
        description: "Using Hamilton's OpenLineageAdapter, you can automatically push data lineage information via any OpenLineage Client.",
        docs_url: "https://hamilton.dagworks.io/",
        org_url: "https://github.com/dagworks-inc/hamilton"
    },
    {
        image: "keboola_logo_lg.svg",
        org: "Keboola",
        full_name: "Keboola",
        description: "Keboola's OpenLineage integration automatically pushes all job information to an OpenLineage-compatible API endpoint.",
        docs_url: "https://app.swaggerhub.com/apis-docs/keboola/job-queue-api/1.3.1#/Jobs/getJobOpenApiLineage",
        org_url: "https://docs.google.com/presentation/d/e/2PACX-1vTCfQcWUM_9e-lNlBqtaWLPjQ7ihvwHPjq0sJ47eJjjc0zNoLXlWOdcznE90t6IVNGBWFwGZBoU-d-o/pub?start=true&loop=true&delayms=3000&slide=id.g136261d2e68_0_1"
    },
    {
        image: "snowflake_logo.svg",
        org: "Snowflake",
        full_name: "Snowflake",
        description: "Snowflake's OpenLineage Adapter creates an account-scoped view from ACCESS_HISTORY and QUERY_HISTORY to output each query that accesses tables in OpenLineage JsonSchema specification.",
        docs_url: "https://github.com/Snowflake-Labs/OpenLineage-AccessHistory-Setup",
        org_url: "https://developers.snowflake.com/"
    },
    {
        image: "spark_logo.svg",
        org: "Spark",
        full_name: "Spark",
        description: "The OpenLineage Spark Agent uses jvm instrumentation to emit OpenLineage metadata.",
        docs_url: "",
        org_url: "https://spark.apache.org"
    },
    {
        image: "trino_og.svg",
        org: "Trino",
        full_name: "Trino",
        description: "The OpenLineage event listener plugin allows streaming of lineage information, encoded in JSON format aligned with OpenLineage specification, to an external, OpenLineage copmpatible API, by POSTing them to a specified URI.",
        docs_url: "https://trino.io/docs/current/admin/event-listeners-openlineage.html",
        org_url: "https://trino.io/"
    },
    {
        image: "Logocombo_SnapLogic_RGB.svg",
        org: "SnapLogic",
        full_name: "SnapLogic",
        description: "SnapLogic's Data Lineage feature supports automated collection of data lineage metadata from SnapLogic Pipelines. This metadata is made available in Open Lineage format to the consumers.",
        docs_url: "https://www.snaplogic.com/blog/snaplogic-loves-openlineage",
        org_url: "https://www.snaplogic.com/"
    },
    {
        image: "oleander_logo.svg",
        org: "oleander",
        full_name: "oleander",
        description: "Data Observability. Simplified. Application Performance Monitoring for Data Pipelines.",
        docs_url: "https://docs.oleander.dev/",
        org_url: "https://oleander.dev/"
    },
]
