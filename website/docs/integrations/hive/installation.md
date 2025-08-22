---
sidebar_position: 2
title: Installation
---

:::info
This does not demonstrate how to configure the `HiveOpenLineageHook`.
Please refer to the [Configuration](configuration/configuration.md) section.
:::

:::info
Currently we only support Hive 3
:::

:::warning
In case of using the Hive integration on [Google Cloud Dataproc](https://cloud.google.com/dataproc) see [Dataproc installation](#dataproc-installation)
:::

To integrate OpenLineage Hive, you can:

- [Place the JAR in your hive lib directory](#place-the-jar-in-your-hive-lib-directory)
- [Add jar in your session](#add-jar-in-your-session)

#### Place the JAR in your hive lib directory

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification, move the JAR to hive lib directory e.g. `/usr/lib/hadoop/lib`.

#### Add jar in your session

1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification put the jar on your cluster (your hdfs or local).
4. Inside the session execute
   1. For jars on local fs - `add jar file:///path/to/my.jar`
   2. For jars on hdfs - `add jar hdfs:///path/to/my.jar`

#### Dataproc installation
:::info
Dataproc has a support Hive Openlineage integration by default, to use that see [here](https://cloud.google.com/dataproc/docs/guides/hive-lineage#enable-hive-data-lineage)

In case you want to use non-default version of OpenLineage you need to add it during cluster creation to avoid potential classloading issues:
:::
1. Download the JAR and its checksum from Maven Central.
2. Verify the JAR's integrity using the checksum.
3. Upon successful verification put the jar on GCS bucket
4. Put [initialization script](#initialization-script) on GCS bucket
5. During cluster creation define initialization script and metadata

```shell
gcloud dataproc clusters create <cluster_name> \
  --zone <zone> \
  --region <region> \
  --scopes cloud-platform \
  --initialization-actions gs://<your_bucket>/path/to/initialization_script.sh \
  --metadata "jar-urls=gs://<your_bucket>/path/to/openlineage-hive.jar" \
  --properties "hive:hive.server2.session.hook=io.openlineage.hive.hooks.HiveOpenLineageHook" \
  --properties "hive:hive.exec.post.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook" \
  --properties "hive:hive.exec.failure.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook" \
  --properties "hive:hive.conf.validation=false" \
  --properties "hive:hive.openlineage.namespace=mynamespace" \
  --properties "hive:hive.openlineage.transport.type=gcplineage" \
  --properties "hive:hive.openlineage.transport.projectId=${PROJECT}" \
  --properties "hive:hive.openlineage.transport.location=us"
```

#### Initialization script
Example initialization script

```bash
#!/bin/bash

set -euxo pipefail

readonly VM_HADOOP_LIB_DIR=/usr/lib/hadoop/lib
readonly VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR=/usr/local/share/google/dataproc/lib
readonly JAR_URLS=$(/usr/share/google/get_metadata_value attributes/jar-urls || true)

if [[ -d ${VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR} ]]; then
  vm_lib_dir=${VM_DATAPROC_VM_HADOOP_LIB_DIR_DIR}
else
  vm_lib_dir=${VM_HADOOP_LIB_DIR}
fi

IFS=',' read -ra URLS <<< "$JAR_URLS"
for url in "${URLS[@]}"; do
    gsutil cp -P "$url" "$vm_lib_dir/"
    if [ $? -eq 0 ]; then
        echo "Successfully copied $url to $vm_lib_dir/"
    else
        echo "Failed to copy $url to $vm_lib_dir/"
    fi
done
```