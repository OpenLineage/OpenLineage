# Terraform OpenLineage Provider - Example Configuration

This directory contains a complete example of using the OpenLineage Terraform provider.

## File Structure

```
examples/
├── main.tf       # Provider configuration
├── datasets.tf   # Dataset resource definitions (6 datasets)
├── jobs.tf       # Job resource definitions (2 jobs)
└── README.md     # This file
```

## Architecture

### Datasets

**PostgreSQL Source Tables:**
1. **customers** - Customer master data
2. **orders** - Order transactions
3. **products** - Product catalog
4. **order_items** - Order line items

**BigQuery Analytics Tables:**
5. **customer_order_summary** - Aggregated customer statistics
6. **product_sales_summary** - Aggregated product sales statistics

### Jobs

**Job 1: aggregate_customer_orders**
- **Inputs:** customers (1), orders (2)
- **Output:** customer_order_summary (3)
- **Logic:** Joins customers and orders, calculates total orders and spending per customer

**Job 2: aggregate_product_sales**
- **Inputs:** products (4), order_items (5)
- **Output:** product_sales_summary (6)
- **Logic:** Joins products and order items, calculates sales volume and revenue per product

### Data Flow Diagram

```
PostgreSQL                    BigQuery
┌─────────────┐              ┌──────────────────────┐
│  customers  │─┐            │ customer_order_      │
│  (dataset 1)│ │            │ summary (dataset 3)  │
└─────────────┘ │            └──────────────────────┘
                ├─────Job 1────────►
┌─────────────┐ │
│  orders     │─┘
│  (dataset 2)│
└─────────────┘

┌─────────────┐              ┌──────────────────────┐
│  products   │─┐            │ product_sales_       │
│  (dataset 4)│ │            │ summary (dataset 6)  │
└─────────────┘ │            └──────────────────────┘
                ├─────Job 2────────►
┌─────────────┐ │
│ order_items │─┘
│  (dataset 5)│
└─────────────┘
```

## Usage

### 1. Build the Provider

```bash
# From the root of terraform-provider-openlineage/
cd ..
go mod download
go build -o terraform-provider-openlineage
```

### 2. Configure Local Provider

Create or edit `~/.terraformrc`:

```hcl
provider_installation {
  dev_overrides {
    "tomasznazarewicz/openlineage" = "/absolute/path/to/terraform-provider-openlineage"
  }
  direct {}
}
```

### 3. Initialize and Apply

```bash
cd examples

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Apply the configuration
terraform apply
```

### 4. Inspect Results

```bash
# Show all resources
terraform show

# List all resources
terraform state list

# Show specific dataset
terraform state show openlineage_dataset.customers

# Show specific job
terraform state show openlineage_job.aggregate_customer_orders
```

## What Gets Created

### On `terraform apply`:

1. **6 Dataset Resources** are stored in Terraform state with:
   - Full schema definitions
   - Ownership information
   - Storage metadata
   - Tags

2. **2 Job Resources** are created with:
   - Unique `run_id` (UUID) generated
   - Timestamp in `last_run_at`
   - References to input/output datasets (namespace + name)
   - Job metadata (SQL, owners, tags)

3. **Event Emission** (placeholder) is triggered for each job

## Testing Scenarios

### Test Initial Creation
```bash
terraform apply
# All resources created, jobs emit events with initial run_id
```

### Test Job Update
```bash
# Modify a job (e.g., change description in jobs.tf)
# Then apply
terraform apply
# Jobs get new run_id, emit new events
```

### Test Dataset Update
```bash
# Modify a dataset (e.g., add a schema field in datasets.tf)
# Then apply
terraform apply
# Dataset updated in state
# Jobs that reference it are NOT automatically re-run
```

### Test Job Recreation
```bash
# Taint a job to force recreation
terraform taint openlineage_job.aggregate_customer_orders
terraform apply
# Job destroyed and recreated with new run_id
```

### Test Cleanup
```bash
terraform destroy
# All resources removed from state
```

## Key Concepts

### Direct Dataset References

Jobs reference datasets using Terraform's native interpolation:

```hcl
inputs {
  namespace = openlineage_dataset.customers.namespace
  name      = openlineage_dataset.customers.name
}
```

**Benefits:**
- ✅ Simple and explicit
- ✅ Terraform manages dependencies automatically
- ✅ Type-safe references
- ✅ Clear lineage in code

### State Management

- **Datasets:** Store complete metadata in state
- **Jobs:** Store run_id, last_run_at, and dataset references
- **No remote state:** Resources are local to Terraform

### Event Emission (Placeholder)

Currently, event emission is placeholder-only:
- Logs messages to Terraform output
- Does NOT actually send to GCP Lineage API
- Framework is ready for real implementation

## Customization

### Change GCP Project

Edit `main.tf`:
```hcl
provider "openlineage" {
  project_id = "your-gcp-project"
  region     = "us-central1"
}
```

### Add More Datasets

Add to `datasets.tf`:
```hcl
resource "openlineage_dataset" "new_dataset" {
  namespace = "..."
  name      = "..."
  # ... schema, etc.
}
```

### Add More Jobs

Add to `jobs.tf` and reference existing datasets:
```hcl
resource "openlineage_job" "new_job" {
  # ...
  inputs {
    namespace = openlineage_dataset.existing_dataset.namespace
    name      = openlineage_dataset.existing_dataset.name
  }
}
```

### Modify Job Logic

Update SQL queries in `jobs.tf`:
```hcl
sql {
  query = <<-SQL
    SELECT ... your new query ...
  SQL
  dialect = "spark"
}
```

## What's Working

✅ Complete resource definitions
✅ Dataset schemas with full metadata
✅ Job inputs/outputs configuration
✅ Terraform dependency management
✅ State tracking (run_id, timestamps)
✅ Resource references and interpolation

## What's Not Yet Implemented

❌ Actual OpenLineage event building
❌ GCP Lineage API integration
❌ Column-level lineage (nested schema)
❌ Dataset enrichment in events
❌ Validation of dataset references

## Troubleshooting

### Provider Not Found
```bash
# Make sure ~/.terraformrc has correct path
# Rebuild provider: go build
```

### Schema Validation Errors
```bash
# Check that all required fields are present
# Validate HCL syntax: terraform validate
```

### State Issues
```bash
# Reset state if needed
rm -rf .terraform terraform.tfstate*
terraform init
```

## Next Steps

1. ✅ Try the example: `terraform apply`
2. 📊 Inspect state: `terraform show`
3. 🔄 Test updates: Modify and reapply
4. 🚀 Implement real event emission
5. 🔗 Add column lineage support

## Documentation

- Main README: `../README.md`
- Implementation Status: `../IMPLEMENTATION_STATUS.md`
- Architecture Guide: `../ARCHITECTURE_EXPLAINED.md`

