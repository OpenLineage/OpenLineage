# Nested Structures Guide: Schema Fields and Tags

This guide explains how to use nested structures for schema fields and tags in the OpenLineage Terraform provider.

## Table of Contents

1. [Nested Schema Fields](#nested-schema-fields)
2. [Hierarchical Tags](#hierarchical-tags)
3. [Common Patterns](#common-patterns)
4. [Best Practices](#best-practices)

---

## Nested Schema Fields

### Overview

Nested schema fields represent complex data types like STRUCT, RECORD, ARRAY, and MAP. They allow you to model hierarchical data structures that exist in BigQuery, Avro, Parquet, and other formats.

### Basic Structure

```hcl
schema_fields {
  name             = "address"
  type             = "STRUCT"
  description      = "User address"
  ordinal_position = 2
  
  # Nested fields
  fields {
    name = "street"
    type = "STRING"
  }
  
  fields {
    name = "city"
    type = "STRING"
  }
}
```

### Supported Nesting Levels

You can nest fields **multiple levels deep**:

```hcl
schema_fields {
  name = "user"
  type = "STRUCT"
  
  fields {
    name = "profile"
    type = "STRUCT"
    
    fields {
      name = "preferences"
      type = "STRUCT"
      
      fields {
        name = "notifications"
        type = "STRUCT"
        
        fields {
          name = "email"
          type = "BOOLEAN"
        }
      }
    }
  }
}
```

### Common Patterns

#### 1. Simple STRUCT

```hcl
schema_fields {
  name = "address"
  type = "STRUCT"
  
  fields {
    name = "street"
    type = "STRING"
  }
  
  fields {
    name = "city"
    type = "STRING"
  }
  
  fields {
    name = "zip_code"
    type = "STRING"
  }
}
```

**Use Case:** Grouping related fields (address, contact info, etc.)

#### 2. ARRAY of STRUCT

```hcl
schema_fields {
  name = "phone_numbers"
  type = "ARRAY<STRUCT>"
  
  fields {
    name = "type"
    type = "STRING"
    description = "mobile, home, work"
  }
  
  fields {
    name = "number"
    type = "STRING"
  }
  
  fields {
    name = "is_primary"
    type = "BOOLEAN"
  }
}
```

**Use Case:** Multiple items of the same structure (phone numbers, addresses, items in order)

#### 3. Nested STRUCT within STRUCT

```hcl
schema_fields {
  name = "user"
  type = "STRUCT"
  
  fields {
    name = "device"
    type = "STRUCT"
    
    fields {
      name = "type"
      type = "STRING"
    }
    
    fields {
      name = "os"
      type = "STRING"
    }
  }
  
  fields {
    name = "location"
    type = "STRUCT"
    
    fields {
      name = "country"
      type = "STRING"
    }
    
    fields {
      name = "city"
      type = "STRING"
    }
  }
}
```

**Use Case:** Complex hierarchical data (user context, event metadata)

#### 4. MAP Types

```hcl
schema_fields {
  name = "properties"
  type = "MAP<STRING, STRING>"
  description = "Key-value pairs"
}
```

**Use Case:** Dynamic properties, flexible metadata

#### 5. ARRAY of Simple Types

```hcl
schema_fields {
  name = "interests"
  type = "ARRAY<STRING>"
  description = "User interests"
}
```

**Use Case:** Lists of values (tags, categories, IDs)

---

## Hierarchical Tags

### Overview

While tags themselves aren't deeply nested in structure, you can create **hierarchical naming conventions** and use the optional `field` and `source` attributes to organize tags logically.

### Basic Structure

```hcl
tags {
  key    = "tag_key"
  value  = "tag_value"
  field  = "column_name"      # Optional: which field this tag applies to
  source = "tag_source"       # Optional: who/what set this tag
}
```

### Hierarchical Naming Patterns

#### 1. Domain Hierarchy

```hcl
tags {
  key   = "domain"
  value = "sales"
}

tags {
  key   = "domain.subdomain"
  value = "ecommerce"
}

tags {
  key   = "domain.team"
  value = "sales-analytics"
}
```

#### 2. Compliance Hierarchy

```hcl
tags {
  key    = "compliance.gdpr"
  value  = "applicable"
  source = "compliance-scanner"
}

tags {
  key    = "compliance.gdpr.retention-days"
  value  = "2555"
  source = "legal-team"
}

tags {
  key    = "compliance.pci-dss"
  value  = "applicable"
  field  = "credit_card_number"
  source = "security-team"
}
```

#### 3. Quality Metrics Hierarchy

```hcl
tags {
  key    = "quality.completeness"
  value  = "99.8"
  source = "data-quality-monitor"
}

tags {
  key    = "quality.accuracy"
  value  = "98.5"
  field  = "order_total"
  source = "data-quality-monitor"
}

tags {
  key    = "quality.timeliness"
  value  = "95.2"
  source = "data-quality-monitor"
}
```

#### 4. Security Classification Hierarchy

```hcl
tags {
  key    = "security.classification"
  value  = "confidential"
  source = "security-team"
}

tags {
  key    = "security.encryption-required"
  value  = "true"
  field  = "ssn"
  source = "security-policy"
}

tags {
  key    = "security.access-level"
  value  = "restricted"
  source = "iam-policy"
}
```

#### 5. Field-Specific Tags

```hcl
# PII tags at field level
tags {
  key   = "pii"
  value = "true"
  field = "email"
}

tags {
  key   = "pii"
  value = "true"
  field = "phone_number"
}

tags {
  key   = "pii.type"
  value = "contact-info"
  field = "email"
}

# Nested field references (for STRUCT types)
tags {
  key   = "pii"
  value = "true"
  field = "user.location.ip_address"
}

tags {
  key   = "pii.encryption"
  value = "required"
  field = "user.location.ip_address"
}
```

---

## Common Patterns

### Pattern 1: Event Data with Nested Context

```hcl
resource "openlineage_dataset" "clickstream" {
  namespace = "kafka://prod:9092"
  name      = "events.clicks"
  
  # Flat event metadata
  schema_fields {
    name = "event_id"
    type = "STRING"
  }
  
  # Nested user context
  schema_fields {
    name = "user"
    type = "STRUCT"
    
    fields {
      name = "user_id"
      type = "STRING"
    }
    
    fields {
      name = "session_id"
      type = "STRING"
    }
  }
  
  # Nested device info
  schema_fields {
    name = "device"
    type = "STRUCT"
    
    fields {
      name = "type"
      type = "STRING"
    }
    
    fields {
      name = "os"
      type = "STRING"
    }
  }
  
  # PII tags for nested fields
  tags {
    key   = "pii"
    value = "true"
    field = "user.user_id"
  }
}
```

### Pattern 2: E-commerce Order with Nested Items

```hcl
resource "openlineage_dataset" "orders_detailed" {
  namespace = "bigquery://project"
  name      = "ecommerce.orders_with_items"
  
  schema_fields {
    name = "order_id"
    type = "STRING"
  }
  
  # Array of order items
  schema_fields {
    name = "items"
    type = "ARRAY<STRUCT>"
    
    fields {
      name = "product_id"
      type = "STRING"
    }
    
    fields {
      name = "quantity"
      type = "INT64"
    }
    
    fields {
      name = "price"
      type = "NUMERIC"
    }
    
    # Nested product details
    fields {
      name = "product"
      type = "STRUCT"
      
      fields {
        name = "name"
        type = "STRING"
      }
      
      fields {
        name = "category"
        type = "STRING"
      }
    }
  }
  
  tags {
    key   = "domain"
    value = "ecommerce"
  }
  
  tags {
    key    = "quality.completeness"
    value  = "100"
    field  = "items"
    source = "data-quality"
  }
}
```

### Pattern 3: JSON/Dynamic Data

```hcl
resource "openlineage_dataset" "api_logs" {
  namespace = "s3://logs"
  name      = "api/request_logs"
  
  schema_fields {
    name = "request_id"
    type = "STRING"
  }
  
  # Nested request data
  schema_fields {
    name = "request"
    type = "STRUCT"
    
    fields {
      name = "method"
      type = "STRING"
    }
    
    fields {
      name = "path"
      type = "STRING"
    }
    
    fields {
      name = "headers"
      type = "MAP<STRING, STRING>"
    }
    
    fields {
      name = "query_params"
      type = "MAP<STRING, STRING>"
    }
  }
  
  # Nested response data
  schema_fields {
    name = "response"
    type = "STRUCT"
    
    fields {
      name = "status_code"
      type = "INT64"
    }
    
    fields {
      name = "body"
      type = "STRING"
    }
  }
}
```

---

## Best Practices

### For Nested Schema Fields

1. **Keep nesting depth reasonable** - 3-4 levels max for readability
2. **Document complex structures** - Use descriptions liberally
3. **Use consistent naming** - snake_case for field names
4. **Model actual data structure** - Match your source data format
5. **Consider performance** - Deeply nested queries can be slow

### For Tags

1. **Use dot notation** for hierarchies: `domain.subdomain.detail`
2. **Always specify source** for automated tags: `source = "scanner"`
3. **Tag at field level** when applicable: `field = "email"`
4. **Use consistent prefixes**: `compliance.*`, `security.*`, `quality.*`
5. **Document tag meanings** in a central registry

### Naming Conventions

**Schema Fields:**
```
user_id          # Simple field
user.profile     # Nested field (dot notation in field path)
user.addresses   # Array/repeated field
```

**Tags:**
```
domain                          # Top-level category
domain.subdomain                # Hierarchy
compliance.gdpr                 # Specific compliance type
quality.completeness            # Metric category
security.classification         # Security level
```

---

## Implementation Notes

⚠️ **Current Status:**
- Nested schema fields schema is **defined but may not be fully implemented** yet
- Tags with `field` and `source` are supported
- Deep nesting (3+ levels) may need testing

📝 **Recommendations:**
- Test with 1-2 levels of nesting first
- Use the examples in `nested_structures.tf` as templates
- Report any issues with deeply nested structures

---

## See Also

- `nested_structures.tf` - Complete examples
- `datasets.tf` - Basic dataset examples
- `jobs_with_column_lineage.tf` - Column lineage examples
# Nested Structure Examples for Schema and Tags
# ==============================================

# Example 1: Nested Schema Fields (STRUCT/RECORD types)
# Shows how complex nested data types can be represented

resource "openlineage_dataset" "user_profile_with_nested_schema" {
  namespace = "bigquery://gcp-open-lineage-testing"
  name      = "analytics.user_profile_advanced"
  
  documentation = "User profile with nested address and preferences"
  
  data_source {
    name = "bigquery-analytics"
    uri  = "bigquery://gcp-open-lineage-testing/analytics"
  }

  # Simple field
  schema_fields {
    name             = "user_id"
    type             = "INT64"
    description      = "Unique user identifier"
    ordinal_position = 1
  }

  # Nested field: address (STRUCT type)
  schema_fields {
    name             = "address"
    type             = "STRUCT"
    description      = "User address information"
    ordinal_position = 2
    
    # Nested fields within address
    fields {
      name             = "street"
      type             = "STRING"
      description      = "Street address"
      ordinal_position = 1
    }
    
    fields {
      name             = "city"
      type             = "STRING"
      ordinal_position = 2
    }
    
    fields {
      name             = "state"
      type             = "STRING"
      ordinal_position = 3
    }
    
    fields {
      name             = "zip_code"
      type             = "STRING"
      ordinal_position = 4
    }
    
    # Nested STRUCT within STRUCT
    fields {
      name             = "coordinates"
      type             = "STRUCT"
      description      = "Geographic coordinates"
      ordinal_position = 5
      
      fields {
        name = "latitude"
        type = "FLOAT64"
      }
      
      fields {
        name = "longitude"
        type = "FLOAT64"
      }
    }
  }

  # Array of STRUCT (REPEATED STRUCT)
  schema_fields {
    name             = "phone_numbers"
    type             = "ARRAY<STRUCT>"
    description      = "User phone numbers"
    ordinal_position = 3
    
    fields {
      name = "type"
      type = "STRING"
      description = "Phone type: mobile, home, work"
    }
    
    fields {
      name = "number"
      type = "STRING"
    }
    
    fields {
      name = "is_primary"
      type = "BOOLEAN"
    }
  }

  # Complex nested preferences
  schema_fields {
    name             = "preferences"
    type             = "STRUCT"
    ordinal_position = 4
    
    fields {
      name = "notifications"
      type = "STRUCT"
      
      fields {
        name = "email"
        type = "BOOLEAN"
      }
      
      fields {
        name = "sms"
        type = "BOOLEAN"
      }
      
      fields {
        name = "push"
        type = "BOOLEAN"
      }
    }
    
    fields {
      name = "interests"
      type = "ARRAY<STRING>"
      description = "User interests/categories"
    }
    
    fields {
      name = "settings"
      type = "MAP<STRING, STRING>"
      description = "Key-value settings"
    }
  }

  owners {
    name = "team:user-data"
    type = "MAINTAINER"
  }

  storage {
    storage_layer = "bigquery"
    file_format   = "table"
  }

  dataset_type {
    dataset_type = "TABLE"
  }
}

# Example 2: Hierarchical Tags with Nested Context
# Shows how tags can have hierarchical/nested information

resource "openlineage_dataset" "orders_with_hierarchical_tags" {
  namespace = "postgres://prod-db.example.com:5432"
  name      = "ecommerce.public.orders_tagged"
  
  documentation = "Orders with comprehensive tagging"
  
  data_source {
    name = "prod-postgresql"
    uri  = "postgres://prod-db.example.com:5432/ecommerce"
  }

  schema_fields {
    name = "order_id"
    type = "BIGINT"
  }

  schema_fields {
    name = "customer_id"
    type = "BIGINT"
  }

  schema_fields {
    name = "order_total"
    type = "DECIMAL(10,2)"
  }

  owners {
    name = "team:sales"
    type = "MAINTAINER"
  }

  storage {
    storage_layer = "postgres"
    file_format   = "table"
  }

  dataset_type {
    dataset_type = "TABLE"
  }

  # Simple tags
  tags {
    key   = "domain"
    value = "sales"
  }

  tags {
    key   = "criticality"
    value = "high"
  }

  # Field-specific PII tag
  tags {
    key    = "pii"
    value  = "true"
    field  = "customer_id"
    source = "data-governance-team"
  }

  # Compliance tags with source
  tags {
    key    = "compliance.gdpr"
    value  = "applicable"
    source = "compliance-scanner"
  }

  tags {
    key    = "compliance.pci-dss"
    value  = "applicable"
    field  = "order_total"
    source = "compliance-scanner"
  }

  tags {
    key    = "compliance.data-retention-days"
    value  = "2555"
    source = "legal-team"
  }

  # Quality tags with source
  tags {
    key    = "quality.completeness"
    value  = "99.8"
    source = "data-quality-monitor"
  }

  tags {
    key    = "quality.accuracy"
    value  = "98.5"
    field  = "order_total"
    source = "data-quality-monitor"
  }

  # Security classification tags
  tags {
    key    = "security.classification"
    value  = "confidential"
    source = "security-team"
  }

  tags {
    key    = "security.encryption-required"
    value  = "true"
    field  = "customer_id"
    source = "security-team"
  }

  # Lifecycle tags
  tags {
    key    = "lifecycle.archival-tier"
    value  = "hot"
    source = "data-lifecycle-policy"
  }

  tags {
    key    = "lifecycle.last-accessed"
    value  = "2026-02-15"
    source = "usage-tracker"
  }
}

# Example 3: Event/Streaming Dataset with Nested Schema
# Shows Kafka/Avro-style nested schemas

resource "openlineage_dataset" "user_events_stream" {
  namespace = "kafka://prod-kafka.example.com:9092"
  name      = "events.user_events"
  
  documentation = "Real-time user event stream with nested payload"
  
  data_source {
    name = "kafka-events"
    uri  = "kafka://prod-kafka.example.com:9092"
  }

  # Event metadata
  schema_fields {
    name = "event_id"
    type = "STRING"
  }

  schema_fields {
    name = "timestamp"
    type = "TIMESTAMP"
  }

  schema_fields {
    name = "event_type"
    type = "STRING"
    description = "click, view, purchase, etc."
  }

  # Nested user context
  schema_fields {
    name = "user"
    type = "STRUCT"
    
    fields {
      name = "user_id"
      type = "STRING"
    }
    
    fields {
      name = "session_id"
      type = "STRING"
    }
    
    fields {
      name = "device"
      type = "STRUCT"
      
      fields {
        name = "type"
        type = "STRING"
      }
      
      fields {
        name = "os"
        type = "STRING"
      }
      
      fields {
        name = "browser"
        type = "STRING"
      }
    }
    
    fields {
      name = "location"
      type = "STRUCT"
      
      fields {
        name = "country"
        type = "STRING"
      }
      
      fields {
        name = "region"
        type = "STRING"
      }
      
      fields {
        name = "city"
        type = "STRING"
      }
      
      fields {
        name = "ip_address"
        type = "STRING"
      }
    }
  }

  # Nested event payload (varies by event_type)
  schema_fields {
    name = "payload"
    type = "STRUCT"
    
    fields {
      name = "page_url"
      type = "STRING"
    }
    
    fields {
      name = "referrer"
      type = "STRING"
    }
    
    fields {
      name = "properties"
      type = "MAP<STRING, STRING>"
      description = "Dynamic event properties"
    }
    
    fields {
      name = "custom_dimensions"
      type = "ARRAY<STRUCT>"
      
      fields {
        name = "key"
        type = "STRING"
      }
      
      fields {
        name = "value"
        type = "STRING"
      }
    }
  }

  owners {
    name = "team:events"
    type = "MAINTAINER"
  }

  storage {
    storage_layer = "kafka"
    file_format   = "avro"
  }

  dataset_type {
    dataset_type = "STREAM"
  }

  # Field-level tags for PII
  tags {
    key   = "pii"
    value = "true"
    field = "user.user_id"
  }

  tags {
    key   = "pii"
    value = "true"
    field = "user.location.ip_address"
  }

  tags {
    key   = "pii.encryption"
    value = "required"
    field = "user.location.ip_address"
  }
}

