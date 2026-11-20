resource "openlineage_dataset" "test" {
  namespace = "s3://warehouse"
  name      = "all-facets"

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
