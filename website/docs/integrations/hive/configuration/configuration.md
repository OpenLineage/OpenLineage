---
sidebar_position: 1
title: Configuration
---

Configuring the OpenLineage Hive integration is straightforward. 
It uses built-in Hive configuration mechanisms. The most important part of the configuration is setting `hive.exec.post.hooks` and `hive.exec.failure.hooks` to `io.openlineage.hive.hooks.HiveOpenLineageHook` so that Hook can be invoked

Your options are:

1. [Setting the properties directly in SQL](#setting-the-properties-directly-in-sql).
2. [Using `--hiveconf` options with the CLI](#using---hiveconf-options-with-the-cli).
3. [Adding properties to the `hive-site.xml` file](#adding-properties-to-the-hive-site-file).

#### Setting the properties directly in SQL
You can set properties in SQL session with
```sql
SET hive.exec.post.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook
SET hive.exec.failure.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook
SET hive.openlineage.namespace=mynamespace;
SET hive.openlineage.job.name=myname;
SET hive.openlineage.transport.type=console;

SELECT ...
```


#### Using `--hiveconf` options with the CLI

Executing hive query from CLI you can set configuration with `--hiveconf`
```bash
hive \
  --hiveconf "hive.exec.post.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook" \
  --hiveconf "hive.exec.failure.hooks=io.openlineage.hive.hooks.HiveOpenLineageHook" \
  --hiveconf "hive.openlineage.namespace=mynamespace" \
  --hiveconf "hive.openlineage.job.name=myname" \
  --hiveconf "hive.openlineage.transport.type=console" \
  # ... other options
```

:::info
In case of using the Hive integration on [Google Cloud Dataproc](https://cloud.google.com/dataproc) you can use gcloud `--properties`


```shell
gcloud dataproc jobs submit hive \
    --cluster <cluster_name> \
    --region "<region>" \
    --properties "hive.openlineage.job.name=monthly_transaction_summary_job" \
    --execute "<query_string>"
```
:::

#### Adding properties to the `hive-site` file

`hive-site.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    ...
    <property>
        <!-- required only to capture session creation time, can be omitted -->
        <name>hive.server2.session.hook</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>
    <property>
        <name>hive.exec.post.hooks</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>
    <property>
        <name>hive.exec.failure.hooks</name>
        <value>io.openlineage.hive.hooks.HiveOpenLineageHook</value>
    </property>
    <property>
        <name>hive.openlineage.namespace</name>
        <value>mynamespace</value>
    </property>
    <property>
        <name>hive.openlineage.job.name</name>
        <value>myname</value>
    </property>
    <property>
        <name>hive.openlineage.transport.type</name>
        <value>console</value>
    </property>
    ...
</configuration>
```
