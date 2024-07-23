---
sidebar_position: 5
title: Manually Annotated Lineage
---

:::caution
This page is about Airflow's external integration that works mainly for Airflow versions <2.7. 
[If you're using Airflow 2.7+, look at native Airflow OpenLineage provider documentation.](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/index.html)  <br /><br /> 

The ongoing development and enhancements will be focused on the `apache-airflow-providers-openlineage` package, 
while the `openlineage-airflow` will primarily be updated for bug fixes. See [all Airflow versions supported by this integration](older.md#supported-airflow-versions)
:::

:::caution
This feature is only supported with Airflow versions greater than 2.1.0)
:::

Airflow allows operators to track lineage by specifying the input and outputs of the operators via inlets and outlets. OpenLineage tries to find the input and output datasets of the Airflow job via provided extractors or custom extractors. As fallback, if it fails to find any input or output datasets, then OpenLineage defaults to inlets and outlets of Airflow jobs. 


OpenLineage supports automated lineage extraction only for selective operators. For other operators and custom-defined ones, users need to write their own custome extractors (by implementing `extract` / `extract_on_complete` method) for Airflow operators that indicate the input and output dataset of the corresponding task. 
This can be circumvented by specifying the input and output datasets using operator's inlets and outlets. OpenLineage will default to use inlets and outlets as input/output datasets if it cannot find any successful extraction from the extractors. 

While specifying the DAG, inlets and outlets can be provided as lists of Tables for every operator. 

:::note
Airflow supports inlets and outlets to be either a Table, Column, File or User entity. However, currently OpenLineage only extracts lineage via Table entity*
:::

## Example

An operator insider the Airflow DAG can be annotated with inlets and outlets like - 

```
"""Example DAG demonstrating the usage of the extraction via Inlets and Outlets."""

import pendulum
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.lineage.entities import Table, File

def create_table(cluster, database, name):
    return Table(
        database=database,
        cluster=cluster,
        name=name,
    )

t1 = create_table("c1", "d1", "t1")
t2 = create_table("c1", "d1", "t2")
t3 = create_table("c1", "d1", "t3")
t4 = create_table("c1", "d1", "t4")
f1 = File(url = "http://randomfile")

with DAG(
    dag_id='example_operator',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=60),
    params={"example_key": "example_value"},
) as dag:
    task1 = BashOperator(
        task_id='task_1_with_inlet_outlet',
        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        inlets=[t1, t2],
        outlets=[t3],
    )

    task2 = BashOperator(
        task_id='task_2_with_inlet_outlet',
        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        inlets=[t3, f1],
        outlets=[t4],
    )

    task1 >> task2
    
if __name__ == "__main__":
    dag.cli()
```

---

The corresponding lineage graph will be - 


<img width="1792" alt="marquez_lineage" src="https://user-images.githubusercontent.com/32615205/181394536-ad6d516d-a894-4bac-9b57-353c1092492f.png" />

(The image is shown with the **Marquez** UI (metadata collector of OpenLineage events). More info can be found [here](https://marquezproject.github.io/marquez/).

Also note that the *File* entity is not captured by the lineage event currently. 

--- 

## Conversion from Airflow Table entity to Openlineage Dataset 

The naming convention followed here is: 
1. `CLUSTER` of the table entity becomes the namespace of OpenLineage's Dataset
2. The name of the dataset is formed by `{{DATABASE}}.{{NAME}}` where `DATABASE` and `NAME` are attributes specified by Airflow's Table entity. 
