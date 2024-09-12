---
sidebar_position: 4
---

# Using Marquez with dbt

#### Adapted from a [blog post](https://openlineage.io/blog/dbt-with-marquez/) by Ross Turk

:::caution
This guide was developed using an **earlier version** of this integration and may require modification.
:::

Each time it runs, dbt generates a trove of metadata about datasets and the work it performs with them. This tutorial covers the harvesting and effective use of this metadata. For data, the tutorial makes use of the Stackoverflow public data set in BigQuery. The end-product will be two tables of data about trends in Stackoverflow discussions of ELT.

### Prerequisites

- dbt
- Docker Desktop
- git
- Google Cloud Service account  
- Google Cloud Service account JSON key file

Note: your Google Cloud account should have access to BigQuery and read/write access to your GCS bucket. Giving your key file an easy-to-remember name (bq-dbt-demo.json) is recommended. Finally, if using macOS Monterey (macOS 12), you will need to release port 5000 by [disabling the AirPlay Receiver](https://developer.apple.com/forums/thread/682332).

### Instructions

First, run through this excellent [dbt tutorial](https://docs.getdbt.com/tutorial/setting-up). It explains how to create a BigQuery project, provision a service account, download a JSON key, and set up a local dbt environment. The rest of this example assumes the existence of a BigQuery project where models can be run, as well as proper configuration of dbt to connect to the project.

Next, start a local Marquez instance to store lineage metadata. Make sure Docker is running, and then clone the Marquez repository:

```
git clone https://github.com/MarquezProject/marquez.git && cd marquez
./docker/up.sh
```

Check to make sure Marquez is up by visiting http://localhost:3000. The page should display an empty Marquez instance and a message saying there is no data. Also, it should be possible to see the server output from requests in the terminal window where Marquez is running. This window should remain open.

Now, in a new terminal window/pane, clone the following GitHub project, which contains some database models:

```
git clone https://github.com/rossturk/stackostudy.git && cd stackostudy
```

Now it is time to install dbt and its integration with OpenLineage. Doing this in a Python virtual environment is recommended. To create one and install necessary packages, run the following commands:

```
python -m venv virtualenv
source virtualenv/bin/activate
pip install dbt dbt-openlineage
```

Keep in mind that dbt learns how to connect to a BigQuery project by looking for a matching profile in `~/.dbt/profiles.yml`. Create or edit this file so it contains a section with the project's BigQuery connection details. Also, point to the location of the JSON key for the service account. Consult [this section](https://docs.getdbt.com/tutorial/create-a-project-dbt-cli#connect-to-bigquery) in the dbt documentation for more help with dbt profiles. At this point, profiles.yml should look something like this:

```
stackostudy:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /Users/rturk/.dbt/dbt-example.json
      project: dbt-example
      dataset: stackostudy
      threads: 1
      timeout_seconds: 300
      location: US
      priority: interactive
```

The `dbt debug` command checks to see that everything has been configured correctly. Running it now should produce output like the following:

```
% dbt debug
Running with dbt=0.20.1
dbt version: 0.20.1
python version: 3.8.12
python path: /opt/homebrew/Cellar/dbt/0.20.1_1/libexec/bin/python3
os info: macOS-11.5.2-arm64-arm-64bit
Using profiles.yml file at /Users/rturk/.dbt/profiles.yml
Using dbt_project.yml file at /Users/rturk/projects/stackostudy/dbt_project.yml
​
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
​
Required dependencies:
 - git [OK found]
​
Connection:
  method: service-account
  database: stacko-study
  schema: stackostudy
  location: US
  priority: interactive
  timeout_seconds: 300
  maximum_bytes_billed: None
  Connection test: OK connection ok
```

### Important Details

Some important conventions should be followed when designing dbt models for use with OpenLineage. Following these conventions will help ensure that OpenLineage collects the most complete metadata possible.

First, any datasets existing outside the dbt project should be defined in a schema YAML file inside the `models/` directory:

```
version: 2
​
sources:
  - name: stackoverflow
    database: bigquery-public-data
    schema: stackoverflow
    tables:
      - name: posts_questions
      - name: posts_answers
      - name: users
      - name: votes
```

This contains the name of the external dataset - in this case, bigquery-public-datasets - and lists the tables that are used by the models in this project. The name of the file does not matter, as long as it ends with .yml and is inside `models/`. Hardcoding dataset and table names into queries can result in incomplete data.

When writing queries, be sure to use the `{{ ref() }}` and `{{ source() }}` jinja functions when referring to data sources. The `{{ ref() }}` function can be used to refer to tables within the same model, and the `{{ source() }}` function refers to tables we have defined in schema.yml. That way, dbt will properly keep track of the relationships between datasets. For example, to select from both an external dataset and one in this model:

```
select * from {{ source('stackoverflow', 'posts_answers') }}
where parent_id in (select id from {{ ref('filtered_questions') }} )
```

