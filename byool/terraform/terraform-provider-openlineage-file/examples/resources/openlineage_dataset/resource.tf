# ──────────────────────────────────────────────────────────────────────────────
# Minimal example — dataset identity only.
# This is the smallest valid openlineage_dataset configuration.
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_dataset" "minimal" {
  namespace = "postgres"
  name      = "mydb.public.orders"
}


# ──────────────────────────────────────────────────────────────────────────────
# Full example — schema, documentation, and ownership.
# Demonstrates every facet enabled by this provider.
# ──────────────────────────────────────────────────────────────────────────────
resource "openlineage_dataset" "orders" {
  namespace = "postgres"
  name      = "mydb.public.orders-full"

  schema {
    fields {
      name = "id"
      type = "INTEGER"
    }
    fields {
      name        = "amount"
      type        = "NUMERIC"
      description = "Order amount in USD"
    }
    fields {
      name = "created_at"
      type = "TIMESTAMP"
    }
  }

  documentation {
    description = "Raw orders table from the production PostgreSQL database."
  }

  ownership {
    owners {
      name = "team:data-engineering"
      type = "OWNER"
    }
  }
}

output "dataset_file_path" {
  value = openlineage_dataset.orders.file_path
}
