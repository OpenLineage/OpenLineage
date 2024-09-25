import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


The OpenLineage client supports configuration through dynamic environment variables.

### Configuring OpenLineage Client via Dynamic Environment Variables

These environment variables must begin with `OPENLINEAGE__`, followed by sections  of the configuration separated by a double underscore `__`.
All values in the environment variables are automatically converted to lowercase, 
and variable names using snake_case (single underscore) are converted into camelCase within the final configuration.

#### Key Features

1. Prefix Requirement: All environment variables must begin with `OPENLINEAGE__`.
2. Sections Separation: Configuration sections are separated using double underscores `__` to form the hierarchy.
3. Lowercase Conversion: Environment variable values are automatically converted to lowercase.
4. CamelCase Conversion: Any environment variable name using single underscore `_` will be converted to camelCase in the final configuration.
5. JSON String Support: You can pass a JSON string at any level of the configuration hierarchy, which will be merged into the final configuration structure.
6. Hyphen Restriction: You cannot use `-` in environment variable names. If a name strictly requires a hyphen, use a JSON string as the value of the environment variable.
7. Precedence Rules:
* Top-level keys have precedence and will not be overwritten by more nested entries.
* For example, `OPENLINEAGE__TRANSPORT='{..}'` will not have its keys overwritten by `OPENLINEAGE__TRANSPORT__AUTH__KEY='key'`.

#### Examples

<Tabs groupId="configs">
<TabItem value="basic" label="Basic Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=http
OPENLINEAGE__TRANSPORT__URL=http://localhost:5050
OPENLINEAGE__TRANSPORT__ENDPOINT=/api/v1/lineage
OPENLINEAGE__TRANSPORT__AUTH__TYPE=api_key
OPENLINEAGE__TRANSPORT__AUTH__API_KEY=random_token
OPENLINEAGE__TRANSPORT__COMPRESSION=gzip
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: http
  url: http://localhost:5050
  endpoint: api/v1/lineage
  auth:
    type: api_key
    apiKey: random_token
  compression: gzip
```
</TabItem>

<TabItem value="composite" label="Composite Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=composite
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__TYPE=http
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__URL=http://localhost:5050
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__ENDPOINT=/api/v1/lineage
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__AUTH__TYPE=api_key
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__AUTH__API_KEY=random_token
OPENLINEAGE__TRANSPORT__TRANSPORTS__FIRST__AUTH__COMPRESSION=gzip
OPENLINEAGE__TRANSPORT__TRANSPORTS__SECOND__TYPE=console
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: composite
  transports:
    first:
      type: http
      url: http://localhost:5050
      endpoint: api/v1/lineage
      auth:
        type: api_key
        apiKey: random_token
      compression: gzip
    second:
      type: console
```
</TabItem>

<TabItem value="precedence" label="Precedence Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT='{"type":"console"}'
OPENLINEAGE__TRANSPORT__TYPE=http
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: console
```
</TabItem>

<TabItem value="spark-properties" label="Spark Example">

Setting following environment variables:

```sh
OPENLINEAGE__TRANSPORT__TYPE=kafka
OPENLINEAGE__TRANSPORT__TOPIC_NAME=test
OPENLINEAGE__TRANSPORT__MESSAGE_KEY=explicit-key
OPENLINEAGE__TRANSPORT__PROPERTIES='{"key.serializer": "org.apache.kafka.common.serialization.StringSerializer"}'
```

is equivalent to passing following YAML configuration:
```yaml
transport:
  type: kafka
  topicName: test
  messageKey: explicit-key
  properties:
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
```

Please note that you can't use environment variables to set Spark properties, as they are not part of the configuration hierarchy.
Following environment variable:
```sh
OPENLINEAGE__TRANSPORT__PROPERTIES__KEY__SERIALIZER="org.apache.kafka.common.serialization.StringSerializer"
```
would be equivalent to below YAML structure:
```yaml
transport:
  properties:
    key:
      serializer: org.apache.kafka.common.serialization.StringSerializer
```
which is not a valid configuration for Spark.

</TabItem>

<TabItem value="namespace-resolvers" label="Namespace Resolvers Example">

Setting following environment variables:

```sh
OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__TYPE=hostList
OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__HOSTS='["kafka-prod13.company.com", "kafka-prod15.company.com"]'
OPENLINEAGE__DATASET__NAMESPACE_RESOLVERS__RESOLVED_NAME__SCHEMA=kafka
```

is equivalent to passing following YAML configuration:
```yaml
dataset:
  namespaceResolvers:
    resolvedName:
      type: hostList
      hosts:
        - kafka-prod13.company.com
        - kafka-prod15.company.com
       schema: kafka
```
</TabItem>

</Tabs>