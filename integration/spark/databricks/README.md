<!-- SPDX-License-Identifier: Apache-2.0 -->

# Open Lineage in Databricks

Open Lineage's [Spark Integration](../README.md) can be installed on Databricks leveraging init scripts. Please note, Databricks on Google Cloud does not currently support the DBFS CLI so the proposed solution will not work for Google Cloud until that feature is enabled. 

* [Azure Databricks Init Scripts](https://docs.microsoft.com/en-us/azure/databricks/clusters/init-scripts)
* [GCP Databricks Init Scripts](https://docs.gcp.databricks.com/clusters/init-scripts.html)
* [AWS Databricks Init Scripts](https://docs.databricks.com/clusters/init-scripts.html)

## Databricks Install Instructions

Follow the steps below to enable Open Lineage on Databricks.

* Build the jar via Gradle - or download the [latest release](https://search.maven.org/remote_content?g=io.openlineage&a=openlineage-spark&v=LATEST)
* Configure the Databricks CLI with your desired workspace:
    * [Azure Databricks CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/)
    * [GCP Databricks CLI](https://docs.gcp.databricks.com/dev-tools/cli/index.html)
    * [AWS Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
* Run `upload-to-databricks.sh` or `upload-to-databricks.ps1`
    * This creates a folder in dbfs to store the open lineage jar.
    * Copies the jar to the dbfs folder
    * Copies the init script to the dbfs folder
* Create an interactive or job cluster with the relevant spark configs:
    ```
    spark.openlineage.version v1
    spark.openlineage.namespace YOURNAMESPACE
    spark.openlineage.host https://YOURHOST
    ```
* Set the cluster Init script to be: `dbfs:/databricks/openlineage/open-lineage-init-script.sh`

## Observe Logs in Driver Logs

To troubleshoot or observe the Open Lineage information in Databricks, see the `Log4j output` in the Cluster's Driver Logs.

### Initialization Logs

A successful initialization will emit logs in the Log4j output that look similar to the below.

```
YY/MM/DD HH:mm:ss INFO SparkContext: Registered listener io.openlineage.spark.agent.OpenLineageSparkListener

YY/MM/DD HH:mm:ss INFO OpenLineageContext: Init OpenLineageContext: Args: ArgumentParser(host=https://YOURHOST, version=v1, namespace=YOURNAMESPACE, jobName=default, parentRunId=null, apiKey=Optional.empty) URI: https://YOURHOST/api/v1/lineage

YY/MM/DD HH:mm:ss INFO AsyncEventQueue: Process of event SparkListenerApplicationStart(Databricks Shell,Some(app-XXX-0000),YYYY,root,None,None,None) by listener OpenLineageSparkListener took Xs.
```

### For Every Lineage Event

You'll see a message in the Log4j output that looks like the following snippet.

```
YY/MM/DD HH:mm:ss INFO OpenLineageContext: Lineage completed successfully: ResponseMessage(responseCode=200, body=null, error=null) <... LINEAGE INFO ...>
```
