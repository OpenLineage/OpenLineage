resource "openlineage_job" "test" {
  namespace = "tf-test"
  name      = "all-facets"

  documentation {
    description  = "Job docs"
    content_type = "text/plain"
  }
  job_type {
    processing_type = "BATCH"
    integration     = "TERRAFORM"
    job_type        = "JOB"
    emission_pattern {
      event_trigger      = "EVENT_BASED"
      event_content_mode = "COMPLETE_SNAPSHOT"
    }
  }
  lineage {
    entries {
      dataset {
        namespace = "s3://warehouse"
        name      = "out"
        inputs {
          dataset {
            namespace = "s3://raw"
            name      = "events"
          }
        }
        fields {
          name = "id"
          inputs {
            dataset {
              namespace = "s3://raw"
              name      = "events"
              field     = "event_id"
              transformations {
                type        = "DIRECT"
                subtype     = "IDENTITY"
                description = "copy"
                masking     = false
              }
            }
          }
        }
      }
    }
    entries {
      job {
        namespace = "airflow"
        name      = "downstream"
        inputs {
          job {
            run_id = "0190f0a0-0000-7000-8000-000000000000"
          }
        }
      }
    }
  }
  ownership {
    owners {
      name = "team:data"
      type = "MAINTAINER"
    }
  }
  parent {
    job {
      namespace = "airflow"
      name      = "orders_dag"
    }
    root {
      job {
        namespace = "airflow"
        name      = "orders_root"
      }
    }
  }
  sql {
    query   = "SELECT * FROM raw.events"
    dialect = "spark"
  }
  source_code {
    language    = "python"
    source_code = "print(1)"
  }
  source_code_location {
    type     = "git"
    url      = "https://github.com/example/repo/blob/main/job.py"
    repo_url = "https://github.com/example/repo"
    path     = "job.py"
    version  = "abc123"
    tag      = "v1"
    branch   = "main"
  }
  tags {
    tags {
      key    = "team"
      value  = "data"
      source = "USER"
    }
  }

  inputs {
    namespace = "s3://raw"
    name      = "events"

    catalog {
      framework          = "iceberg"
      type               = "hive"
      name               = "main"
      metadata_uri       = "thrift://metastore:9083"
      warehouse_uri      = "s3://warehouse"
      source             = "spark"
      catalog_properties = { owner = "data" }
    }
    dataset_type {
      dataset_type = "TABLE"
      sub_type     = "EXTERNAL"
    }
    version {
      dataset_version = "42"
    }
    datasource {
      name = "warehouse"
      uri  = "s3://warehouse"
    }
    documentation {
      description  = "Dataset docs"
      content_type = "text/markdown"
    }
    fabric_one_lake {
      workspace_id   = "00000000-0000-0000-0000-000000000001"
      workspace_name = "ws"
      item_id        = "00000000-0000-0000-0000-000000000002"
      item_name      = "lakehouse"
      item_type      = "Lakehouse"
      table_id       = "00000000-0000-0000-0000-000000000003"
      location       = "abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/00000000-0000-0000-0000-000000000002/Tables/t"
    }
    hierarchy {
      hierarchy {
        name = "db"
        type = "DATABASE"
      }
      hierarchy {
        name = "t"
        type = "TABLE"
      }
    }
    lineage {
      inputs {
        dataset {
          namespace = "s3://raw"
          name      = "events"
          transformations {
            type    = "DIRECT"
            subtype = "IDENTITY"
          }
        }
      }
      fields {
        name = "id"
        inputs {
          dataset {
            namespace = "s3://raw"
            name      = "events"
            field     = "event_id"
          }
        }
        inputs {
          job {
            namespace = "airflow"
            name      = "load"
          }
        }
      }
    }
    ownership {
      owners {
        name = "team:data"
        type = "MAINTAINER"
      }
    }
    schema {
      fields {
        name             = "id"
        type             = "BIGINT"
        description      = "Identifier"
        ordinal_position = 1
      }
    }
    storage {
      storage_layer = "iceberg"
      file_format   = "parquet"
    }
    symlinks {
      identifiers {
        namespace = "hive://metastore"
        name      = "db.t"
        type      = "TABLE"
      }
    }
    tags {
      tags {
        key    = "pii"
        value  = "false"
        source = "USER"
        field  = "id"
      }
    }
  }

  outputs {
    namespace = "s3://warehouse"
    name      = "out"

    catalog {
      framework          = "iceberg"
      type               = "hive"
      name               = "main"
      metadata_uri       = "thrift://metastore:9083"
      warehouse_uri      = "s3://warehouse"
      source             = "spark"
      catalog_properties = { owner = "data" }
    }
    dataset_type {
      dataset_type = "TABLE"
      sub_type     = "EXTERNAL"
    }
    version {
      dataset_version = "42"
    }
    datasource {
      name = "warehouse"
      uri  = "s3://warehouse"
    }
    documentation {
      description  = "Dataset docs"
      content_type = "text/markdown"
    }
    fabric_one_lake {
      workspace_id   = "00000000-0000-0000-0000-000000000001"
      workspace_name = "ws"
      item_id        = "00000000-0000-0000-0000-000000000002"
      item_name      = "lakehouse"
      item_type      = "Lakehouse"
      table_id       = "00000000-0000-0000-0000-000000000003"
      location       = "abfss://00000000-0000-0000-0000-000000000001@onelake.dfs.fabric.microsoft.com/00000000-0000-0000-0000-000000000002/Tables/t"
    }
    hierarchy {
      hierarchy {
        name = "db"
        type = "DATABASE"
      }
      hierarchy {
        name = "t"
        type = "TABLE"
      }
    }
    lineage {
      inputs {
        dataset {
          namespace = "s3://raw"
          name      = "events"
          transformations {
            type    = "DIRECT"
            subtype = "IDENTITY"
          }
        }
      }
      fields {
        name = "id"
        inputs {
          dataset {
            namespace = "s3://raw"
            name      = "events"
            field     = "event_id"
          }
        }
        inputs {
          job {
            namespace = "airflow"
            name      = "load"
          }
        }
      }
    }
    ownership {
      owners {
        name = "team:data"
        type = "MAINTAINER"
      }
    }
    schema {
      fields {
        name             = "id"
        type             = "BIGINT"
        description      = "Identifier"
        ordinal_position = 1
      }
    }
    storage {
      storage_layer = "iceberg"
      file_format   = "parquet"
    }
    symlinks {
      identifiers {
        namespace = "hive://metastore"
        name      = "db.t"
        type      = "TABLE"
      }
    }
    tags {
      tags {
        key    = "pii"
        value  = "false"
        source = "USER"
        field  = "id"
      }
    }
    column_lineage {
      fields {
        name = "id"
        input_fields {
          namespace = "s3://raw"
          name      = "events"
          field     = "event_id"
          transformations {
            type    = "DIRECT"
            subtype = "IDENTITY"
          }
        }
      }
      dataset {
        namespace = "s3://raw"
        name      = "events"
        field     = "user_id"
        transformations {
          type    = "INDIRECT"
          subtype = "FILTER"
        }
      }
    }
  }
}
