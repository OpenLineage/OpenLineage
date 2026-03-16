# Column-Level Lineage Example

This file demonstrates how to add column-level lineage to OpenLineage jobs.

## Structure

Column lineage is defined within the `outputs` block of a job using the `column_lineage` block:

```hcl
outputs {
  namespace = "..."
  name      = "..."
  
  column_lineage {
    # Field-level lineage
    fields {
      name = "output_column_name"
      
      input_field {
        namespace = "input_dataset_namespace"
        name      = "input_dataset_name"
        field     = "input_column_name"
        
        transformation {
          type        = "DIRECT"
          subtype     = "IDENTITY"
          description = "Human-readable description"
          masking     = false
        }
      }
    }
    
    # Dataset-level lineage (for filters, joins)
    dataset {
      namespace = "dataset_namespace"
      name      = "dataset_name"
      field     = "field_name"
      
      transformation {
        type        = "INDIRECT"
        subtype     = "FILTER"
        description = "Human-readable description"
      }
    }
  }
}
```

## Examples in This File

### Job 1: Simple Lineage Patterns

**Direct Mappings (IDENTITY):**
- `customer_id` ← `customers.customer_id` (direct copy)
- `email` ← `customers.email` (direct copy)
- `first_name` ← `customers.first_name` (direct copy)
- `last_name` ← `customers.last_name` (direct copy)

**Aggregations:**
- `total_orders` ← `COUNT(orders.order_id)`
- `total_spent` ← `SUM(orders.order_total)`

**Dataset-Level Operations:**
- JOIN on `customers.customer_id = orders.customer_id`
- FILTER on `orders.order_status = 'completed'`

### Job 2: Complex Lineage Patterns

**Multiple Input Fields for Single Output:**
- `total_revenue` derives from BOTH:
  - `order_items.quantity` (aggregated and transformed)
  - `order_items.unit_price` (aggregated and transformed)
  - Shows multiplication transformation before aggregation

**Multiple Transformations on Same Field:**
```hcl
input_field {
  field = "quantity"
  
  transformation {
    type    = "INDIRECT"
    subtype = "AGGREGATION"
  }
  
  transformation {
    type    = "INDIRECT"
    subtype = "TRANSFORMATION"
  }
}
```

## Transformation Types

### DIRECT Transformations
Used when output is a simple copy or minor modification of input:
- **IDENTITY** - Direct copy with no changes
- **TRANSFORMATION** - Simple transformation (e.g., CAST, TRIM)

### INDIRECT Transformations
Used when output is derived through complex operations:
- **AGGREGATION** - SUM, COUNT, AVG, MAX, MIN, etc.
- **JOIN** - Field used in join condition
- **FILTER** - Field used in WHERE/HAVING clause
- **TRANSFORMATION** - Complex calculation (e.g., arithmetic)

## Key Patterns

### 1. Direct Column Copy
```hcl
fields {
  name = "customer_id"
  
  input_field {
    namespace = openlineage_dataset.customers.namespace
    name      = openlineage_dataset.customers.name
    field     = "customer_id"
    
    transformation {
      type    = "DIRECT"
      subtype = "IDENTITY"
    }
  }
}
```

### 2. Simple Aggregation
```hcl
fields {
  name = "total_orders"
  
  input_field {
    namespace = openlineage_dataset.orders.namespace
    name      = openlineage_dataset.orders.name
    field     = "order_id"
    
    transformation {
      type        = "INDIRECT"
      subtype     = "AGGREGATION"
      description = "COUNT of order_id"
    }
  }
}
```

### 3. Calculated Field (Multiple Inputs)
```hcl
fields {
  name = "total_revenue"
  
  # Input 1
  input_field {
    field = "quantity"
    transformation { ... }
    transformation { ... }  # Multiple transformations
  }
  
  # Input 2
  input_field {
    field = "unit_price"
    transformation { ... }
  }
}
```

### 4. JOIN Condition (Dataset-Level)
```hcl
dataset {
  namespace = openlineage_dataset.customers.namespace
  name      = openlineage_dataset.customers.name
  field     = "customer_id"
  
  transformation {
    type        = "INDIRECT"
    subtype     = "JOIN"
    description = "LEFT JOIN on customer_id"
  }
}
```

### 5. FILTER Condition (Dataset-Level)
```hcl
dataset {
  namespace = openlineage_dataset.orders.namespace
  name      = openlineage_dataset.orders.name
  field     = "order_status"
  
  transformation {
    type        = "INDIRECT"
    subtype     = "FILTER"
    description = "WHERE order_status = 'completed'"
  }
}
```

## Benefits

1. **Traceability**: Track exactly which input fields contribute to each output
2. **Impact Analysis**: Understand downstream effects of schema changes
3. **Compliance**: Document data transformations for audit purposes
4. **Debugging**: Quickly identify data quality issues at the column level

## Usage

Replace `jobs.tf` with `jobs_with_column_lineage.tf`:

```bash
# Rename files
mv jobs.tf jobs_simple.tf
mv jobs_with_column_lineage.tf jobs.tf

# Apply
terraform apply
```

## Notes

- Column lineage is **optional** - you can define jobs without it
- Each output field can have **multiple input fields**
- Each input field can have **multiple transformations**
- Dataset-level lineage captures operations that affect multiple columns (JOINs, FILTERs)
- Use descriptive transformation descriptions for documentation

## Current Implementation Status

⚠️ **Note:** The deeply nested schema for column lineage (fields → input_fields → transformations) is defined in the provider code but **not yet fully implemented** in the Terraform schema due to complexity.

The examples above show the **intended structure**. You may need to wait for full schema support or use a simplified approach (e.g., JSON strings) until the nested schema is complete.
# Job Definitions with Column-Level Lineage
# ==========================================

# Job 1: Aggregate customer orders
# Inputs: customers (dataset 1), orders (dataset 2)
# Output: customer_order_summary (dataset 3)
resource "openlineage_job" "aggregate_customer_orders" {
  namespace   = "airflow"
  name        = "analytics.aggregate_customer_orders"
  description = "Join customers and orders to create customer summary statistics"

  job_type {
    processing_type = "BATCH"
    integration     = "BYOL"
    job_type        = "QUERY"
  }

  owners {
    name = "team:data-engineering"
    type = "MAINTAINER"
  }

  sql {
    query = <<-SQL
      SELECT 
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) as total_orders,
        SUM(o.order_total) as total_spent
      FROM customers c
      LEFT JOIN orders o ON c.customer_id = o.customer_id
      WHERE o.order_status = 'completed'
      GROUP BY c.customer_id, c.email, c.first_name, c.last_name
    SQL
    dialect = "spark"
  }

  tags {
    key   = "domain"
    value = "analytics"
  }

  tags {
    key   = "schedule"
    value = "daily"
  }

  tags {
    key   = "priority"
    value = "high"
  }

  # Input 1: customers dataset
  inputs {
    namespace = openlineage_dataset.customers.namespace
    name      = openlineage_dataset.customers.name
  }

  # Input 2: orders dataset
  inputs {
    namespace = openlineage_dataset.orders.namespace
    name      = openlineage_dataset.orders.name
  }

  # Output with column-level lineage
  outputs {
    namespace = openlineage_dataset.customer_order_summary.namespace
    name      = openlineage_dataset.customer_order_summary.name

    # Column lineage shows how output columns are derived from input columns
    column_lineage {
      # Field 1: customer_id (direct copy from customers table)
      fields {
        name = "customer_id"
        
        input_field {
          namespace = openlineage_dataset.customers.namespace
          name      = openlineage_dataset.customers.name
          field     = "customer_id"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from customers.customer_id"
            masking     = false
          }
        }
      }

      # Field 2: email (direct copy from customers table)
      fields {
        name = "email"
        
        input_field {
          namespace = openlineage_dataset.customers.namespace
          name      = openlineage_dataset.customers.name
          field     = "email"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from customers.email"
            masking     = false
          }
        }
      }

      # Field 3: first_name (direct copy from customers table)
      fields {
        name = "first_name"
        
        input_field {
          namespace = openlineage_dataset.customers.namespace
          name      = openlineage_dataset.customers.name
          field     = "first_name"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from customers.first_name"
            masking     = false
          }
        }
      }

      # Field 4: last_name (direct copy from customers table)
      fields {
        name = "last_name"
        
        input_field {
          namespace = openlineage_dataset.customers.namespace
          name      = openlineage_dataset.customers.name
          field     = "last_name"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from customers.last_name"
            masking     = false
          }
        }
      }

      # Field 5: total_orders (aggregation from orders table)
      fields {
        name = "total_orders"
        
        input_field {
          namespace = openlineage_dataset.orders.namespace
          name      = openlineage_dataset.orders.name
          field     = "order_id"
          
          transformation {
            type        = "INDIRECT"
            subtype     = "AGGREGATION"
            description = "COUNT of orders.order_id"
            masking     = false
          }
        }
      }

      # Field 6: total_spent (aggregation from orders table)
      fields {
        name = "total_spent"
        
        input_field {
          namespace = openlineage_dataset.orders.namespace
          name      = openlineage_dataset.orders.name
          field     = "order_total"
          
          transformation {
            type        = "INDIRECT"
            subtype     = "AGGREGATION"
            description = "SUM of orders.order_total"
            masking     = false
          }
        }
      }

      # Dataset-level lineage: JOIN condition
      dataset {
        namespace = openlineage_dataset.customers.namespace
        name      = openlineage_dataset.customers.name
        field     = "customer_id"
        
        transformation {
          type        = "INDIRECT"
          subtype     = "JOIN"
          description = "LEFT JOIN on customers.customer_id"
        }
      }

      dataset {
        namespace = openlineage_dataset.orders.namespace
        name      = openlineage_dataset.orders.name
        field     = "customer_id"
        
        transformation {
          type        = "INDIRECT"
          subtype     = "JOIN"
          description = "LEFT JOIN on orders.customer_id"
        }
      }

      # Dataset-level lineage: FILTER condition
      dataset {
        namespace = openlineage_dataset.orders.namespace
        name      = openlineage_dataset.orders.name
        field     = "order_status"
        
        transformation {
          type        = "INDIRECT"
          subtype     = "FILTER"
          description = "WHERE orders.order_status = 'completed'"
        }
      }
    }
  }
}

# Job 2: Aggregate product sales (with complex column lineage)
# Inputs: products (dataset 4), order_items (dataset 5)
# Output: product_sales_summary (dataset 6)
resource "openlineage_job" "aggregate_product_sales" {
  namespace   = "airflow"
  name        = "analytics.aggregate_product_sales"
  description = "Join products and order items to create product sales statistics"

  job_type {
    processing_type = "BATCH"
    integration     = "BYOL"
    job_type        = "QUERY"
  }

  owners {
    name = "team:data-engineering"
    type = "MAINTAINER"
  }

  sql {
    query = <<-SQL
      SELECT 
        p.product_id,
        p.product_name,
        p.category,
        SUM(oi.quantity) as total_quantity_sold,
        SUM(oi.quantity * oi.unit_price) as total_revenue
      FROM products p
      INNER JOIN order_items oi ON p.product_id = oi.product_id
      GROUP BY p.product_id, p.product_name, p.category
      ORDER BY total_revenue DESC
    SQL
    dialect = "spark"
  }

  tags {
    key   = "domain"
    value = "analytics"
  }

  tags {
    key   = "schedule"
    value = "daily"
  }

  tags {
    key   = "priority"
    value = "medium"
  }

  # Input 1: products dataset
  inputs {
    namespace = openlineage_dataset.products.namespace
    name      = openlineage_dataset.products.name
  }

  # Input 2: order_items dataset
  inputs {
    namespace = openlineage_dataset.order_items.namespace
    name      = openlineage_dataset.order_items.name
  }

  # Output with column-level lineage
  outputs {
    namespace = openlineage_dataset.product_sales_summary.namespace
    name      = openlineage_dataset.product_sales_summary.name

    column_lineage {
      # Field 1: product_id (direct from products)
      fields {
        name = "product_id"
        
        input_field {
          namespace = openlineage_dataset.products.namespace
          name      = openlineage_dataset.products.name
          field     = "product_id"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from products.product_id"
            masking     = false
          }
        }
      }

      # Field 2: product_name (direct from products)
      fields {
        name = "product_name"
        
        input_field {
          namespace = openlineage_dataset.products.namespace
          name      = openlineage_dataset.products.name
          field     = "product_name"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from products.product_name"
            masking     = false
          }
        }
      }

      # Field 3: category (direct from products)
      fields {
        name = "category"
        
        input_field {
          namespace = openlineage_dataset.products.namespace
          name      = openlineage_dataset.products.name
          field     = "category"
          
          transformation {
            type        = "DIRECT"
            subtype     = "IDENTITY"
            description = "Direct mapping from products.category"
            masking     = false
          }
        }
      }

      # Field 4: total_quantity_sold (aggregation from order_items)
      fields {
        name = "total_quantity_sold"
        
        input_field {
          namespace = openlineage_dataset.order_items.namespace
          name      = openlineage_dataset.order_items.name
          field     = "quantity"
          
          transformation {
            type        = "INDIRECT"
            subtype     = "AGGREGATION"
            description = "SUM of order_items.quantity"
            masking     = false
          }
        }
      }

      # Field 5: total_revenue (calculated from multiple fields)
      # This shows a complex case with multiple input fields
      fields {
        name = "total_revenue"
        
        # First input: quantity
        input_field {
          namespace = openlineage_dataset.order_items.namespace
          name      = openlineage_dataset.order_items.name
          field     = "quantity"
          
          transformation {
            type        = "INDIRECT"
            subtype     = "AGGREGATION"
            description = "SUM of (quantity * unit_price)"
            masking     = false
          }
          
          transformation {
            type        = "INDIRECT"
            subtype     = "TRANSFORMATION"
            description = "Multiplication with unit_price"
            masking     = false
          }
        }
        
        # Second input: unit_price
        input_field {
          namespace = openlineage_dataset.order_items.namespace
          name      = openlineage_dataset.order_items.name
          field     = "unit_price"
          
          transformation {
            type        = "INDIRECT"
            subtype     = "AGGREGATION"
            description = "SUM of (quantity * unit_price)"
            masking     = false
          }
          
          transformation {
            type        = "INDIRECT"
            subtype     = "TRANSFORMATION"
            description = "Multiplication with quantity"
            masking     = false
          }
        }
      }

      # Dataset-level lineage: JOIN condition
      dataset {
        namespace = openlineage_dataset.products.namespace
        name      = openlineage_dataset.products.name
        field     = "product_id"
        
        transformation {
          type        = "INDIRECT"
          subtype     = "JOIN"
          description = "INNER JOIN on products.product_id"
        }
      }

      dataset {
        namespace = openlineage_dataset.order_items.namespace
        name      = openlineage_dataset.order_items.name
        field     = "product_id"
        
        transformation {
          type        = "INDIRECT"
          subtype     = "JOIN"
          description = "INNER JOIN on order_items.product_id"
        }
      }
    }
  }
}

