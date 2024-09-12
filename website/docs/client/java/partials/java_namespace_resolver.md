import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info
This feature is available in OpenLineage 1.17 and above
:::

Oftentimes host addresses are used to access data and a single dataset can be accessed via different
addresses. For example, a Kafka topic can be accessed by a list of kafka bootstrap servers or any 
server from the list. In general, a problem can be solved by adding mechanism which resolves host addresses into 
logical identifier understood within the organisation. This applies for all clusters like Kafka or Cassandra
which should be identified regardless of current list of hosts they contain. This also applies
for JDBC urls where a physical address of database can change over time.

### Host List Resolver

Host List Resolver given a list of hosts, replaces host name within 
the dataset namespace into the resolved value defined.

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
dataset:
  namespaceResolvers:
    resolved-name:
      type: hostList
      hosts: ['kafka-prod13.company.com', 'kafka-prod15.company.com']
      schema: "kafka"
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                                             | Definition    | Example |
------------------- ----------------------------------------------------|---------------|--
| spark.openlineage.dataset.namespaceResolvers.resolved-name.type  | Resolver type | hostList |
| spark.openlineage.dataset.namespaceResolvers.resolved-name.hosts | List of hosts | `['kafka-prod13.company.com', 'kafka-prod15.company.com']` |
| spark.openlineage.dataset.namespaceResolvers.resolved-name.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                                    | Definition    | Example |
------------------- -------------------------------------------|---------------|--
| openlineage.dataset.namespaceResolvers.resolved-name.type  | Resolver type | hostList |
| openlineage.dataset.namespaceResolvers.resolved-name.hosts | List of hosts | `['kafka-prod13.company.com', 'kafka-prod15.company.com']` |
| openlineage.dataset.namespaceResolvers.resolved-name.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |


</TabItem>
</Tabs>

### Pattern Namespace Resolver

Java regex pattern is used to identify a host. Substrings matching a pattern will be replaced with resolved name.

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
dataset:
  namespaceResolvers:
    resolved-name:
      type: pattern
      # 'cassandra-prod7.company.com', 'cassandra-prod8.company.com'
      regex: 'cassandra-prod(\d)+\.company\.com'
      schema: "cassandra"
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                                          | Definition    | Example |
------------------- -------------------------------------------------|---------------|--------
| spark.openlineage.dataset.namespaceResolvers.resolved-name.type  | Resolver type | pattern |
| spark.openlineage.dataset.namespaceResolvers.resolved-name.hosts | Regex pattern to find and replace | `cassandra-prod(\d)+\.company\.com` |
| spark.openlineage.dataset.namespaceResolvers.resolved-name.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                                    | Definition    | Example |
------------------- -------------------------------------------|---------------|--
| openlineage.dataset.namespaceResolvers.resolved-name.type  | Resolver type | pattern |
| openlineage.dataset.namespaceResolvers.resolved-name.hosts | Regex pattern to find and replace | `cassandra-prod(\d)+\.company\.com` |
| openlineage.dataset.namespaceResolvers.resolved-name.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |

</TabItem>
</Tabs>

### Pattern Group Namespace Resolver

For this resolver, Java regex pattern is used to identify a host. However, instead of configured resolved name,
a `matchingGroup` is used a resolved name. This can be useful when having several clusters
made from hosts with a well-defined host naming convention.

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
dataset:
  namespaceResolvers:
    test-pattern:
      type: patternGroup
      # 'cassandra-test-7.company.com', 'cassandra-test-8.company.com', 'kafka-test-7.company.com', 'kafka-test-8.company.com'
      regex: '(?<cluster>[a-zA-Z-]+)-(\d)+\.company\.com:[\d]*'
      matchingGroup: "cluster"
      schema: "cassandra"
```
</TabItem>
<TabItem value="spark" label="Spark Config">

| Parameter                                                             | Definition    | Example |
------------------- ----------------------------------------------------|---------------|--
| spark.openlineage.dataset.namespaceResolvers.pattern-group-resolver.type  | Resolver type | patternGroup |
| spark.openlineage.dataset.namespaceResolvers.pattern-group-resolver.regex | Regex pattern to find and replace | `(?<cluster>[a-zA-Z-]+)-(\d)+\.company\.com:[\d]*` |
| spark.openlineage.dataset.namespaceResolvers.pattern-group-resolver.matchingGroup | Matching group named within the regex | `cluster` |
| spark.openlineage.dataset.namespaceResolvers.pattern-group-resolver.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |

</TabItem>
<TabItem value="flink" label="Flink Config">

| Parameter                                                             | Definition    | Example |
------------------- ----------------------------------------------------|---------------|--
| openlineage.dataset.namespaceResolvers.pattern-group-resolver.type  | Resolver type | patternGroup |
| openlineage.dataset.namespaceResolvers.pattern-group-resolver.regex | Regex pattern to find and replace | `(?<cluster>[a-zA-Z-]+)-(\d)+\.company\.com` |
| openlineage.dataset.namespaceResolvers.pattern-group-resolver.matchingGroup | Matching group named within the regex | `cluster` |
| openlineage.dataset.namespaceResolvers.pattern-group-resolver.schema | Optional schema to be specified. Resolver will be only applied if schema matches the configure one. | `kafka` |

</TabItem>
</Tabs>

### Custom Resolver

Custom resolver can be added by implementing:
 * `io.openlineage.client.dataset.namespaceResolver.DatasetNamespaceResolver`
 * `io.openlineage.client.dataset.namespaceResolver.DatasetNamespaceResolverBuilder`
 * `io.openlineage.client.dataset.namespaceResolver.DatasetNamespaceResolverConfig`

Config class can be used to pass any namespace resolver parameters through standard configuration 
mechanism (Spark & Flink configuration or `openlineage.yml` file provided). Standard `ServiceLoader`
approach is used to load and initiate custom classes.