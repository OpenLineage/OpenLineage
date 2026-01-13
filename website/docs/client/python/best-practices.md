---
sidebar_position: 3
title: Best Practices
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Dataset Naming Helpers

The OpenLineage client provides a `naming` module with helper classes for constructing valid dataset 
names and namespaces according to OpenLineage's dataset naming specification. 
These helpers ensure consistent naming across different data platforms.

Each class implementing the `DatasetNaming` protocol takes platform-specific parameters and provides 
`get_namespace()` and `get_name()` methods that return properly formatted namespace and name strings.

### Examples

<Tabs groupId="naming">
<TabItem value="snowflake" label="Snowflake">

```python
from openlineage.client.naming.dataset import Snowflake
from openlineage.client.event_v2 import Dataset

# Create naming helper
naming = Snowflake(
    organization_name="myorg",
    account_name="myaccount",
    database="mydb",
    schema="myschema",
    table="mytable"
)

# Get namespace and name
namespace = naming.get_namespace()  # "snowflake://myorg-myaccount"
name = naming.get_name()  # "mydb.myschema.mytable"

# Use in Dataset
dataset = Dataset(namespace=namespace, name=name)
```

</TabItem>
<TabItem value="bigquery" label="BigQuery">

```python
from openlineage.client.naming.dataset import BigQuery
from openlineage.client.event_v2 import Dataset

# Create naming helper
naming = BigQuery(
    project_id="my-project",
    dataset_name="my_dataset",
    table_name="my_table"
)

# Get namespace and name
namespace = naming.get_namespace()  # "bigquery"
name = naming.get_name()  # "my-project.my_dataset.my_table"

# Use in Dataset
dataset = Dataset(namespace=namespace, name=name)
```

</TabItem>
<TabItem value="s3" label="S3">

```python
from openlineage.client.naming.dataset import S3
from openlineage.client.event_v2 import Dataset

# Create naming helper
naming = S3(
    bucket_name="my-bucket",
    object_key="path/to/file.parquet"
)

# Get namespace and name
namespace = naming.get_namespace()  # "s3://my-bucket"
name = naming.get_name()  # "path/to/file.parquet"

# Use in Dataset
dataset = Dataset(namespace=namespace, name=name)
```

</TabItem>
<TabItem value="postgres" label="Postgres">

```python
from openlineage.client.naming.dataset import Postgres
from openlineage.client.event_v2 import Dataset

# Create naming helper
naming = Postgres(
    host="localhost",
    port="5432",
    database="mydb",
    schema="public",
    table="users"
)

# Get namespace and name
namespace = naming.get_namespace()  # "postgres://localhost:5432"
name = naming.get_name()  # "mydb.public.users"

# Use in Dataset
dataset = Dataset(namespace=namespace, name=name)
```

</TabItem>
<TabItem value="kafka" label="Kafka">

```python
from openlineage.client.naming.dataset import Kafka
from openlineage.client.event_v2 import Dataset

# Create naming helper
naming = Kafka(
    bootstrap_server_host="kafka.example.com",
    port="9092",
    topic="my-topic"
)

# Get namespace and name
namespace = naming.get_namespace()  # "kafka://kafka.example.com:9092"
name = naming.get_name()  # "my-topic"

# Use in Dataset
dataset = Dataset(namespace=namespace, name=name)
```

</TabItem>
</Tabs>

