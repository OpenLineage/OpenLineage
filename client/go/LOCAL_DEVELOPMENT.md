# Using the OpenLineage Go Client Locally

This guide explains how to use the OpenLineage Go client in a separate project during local development.

## Option 1: Using `go mod replace` (Simplest)

In your consuming project, add a replace directive to point to the local module:

```bash
# Navigate to your project
cd /path/to/your/project

# Add the replace directive
go mod edit -replace github.com/OpenLineage/openlineage/client/go=/path/to/OpenLineage/client/go

# Get the packages
go get github.com/OpenLineage/openlineage/client/go/pkg/openlineage
go get github.com/OpenLineage/openlineage/client/go/pkg/transport
```

Your `go.mod` will look like:

```go
module your-project

go 1.24.0

require (
    github.com/OpenLineage/openlineage/client/go v0.0.0
)

replace github.com/OpenLineage/openlineage/client/go => /path/to/OpenLineage/client/go
```

## Option 2: Using Go Workspace (Recommended for Multi-Module Development)

Go workspaces allow you to work on multiple modules simultaneously without modifying `go.mod` files.

```bash
# Create a parent directory for your workspace
mkdir workspace && cd workspace

# Clone or link both projects
git clone https://github.com/OpenLineage/OpenLineage.git
git clone https://github.com/your-org/your-project.git

# Initialize workspace
go work init

# Add both modules to the workspace
go work use ./OpenLineage/client/go
go work use ./your-project
```

The generated `go.work` file will look like:

```go
go 1.24.0

use (
    ./OpenLineage/client/go
    ./your-project
)
```

Now your project will automatically use the local OpenLineage client without any `replace` directives.

## Option 3: Using a Local Git Pseudo-Version

For testing before committing:

```bash
# In the OpenLineage repo, get the current commit hash
cd /path/to/OpenLineage
git rev-parse --short HEAD  # e.g., abc1234

# In your project, reference the local path
go mod edit -replace github.com/OpenLineage/openlineage/client/go=/path/to/OpenLineage/client/go
```

## Example Usage

```go
package main

import (
    "context"
    "log"

    ol "github.com/OpenLineage/openlineage/client/go/pkg/openlineage"
    "github.com/OpenLineage/openlineage/client/go/pkg/transport"
)

func main() {
    // Create client with GCP Lineage transport
    client, err := ol.NewClient(ol.ClientConfig{
        Namespace: "my-namespace",
        Transport: transport.Config{
            Type: transport.TransportTypeGCPLineage,
            GCPLineage: transport.GCPLineageConfig{
                ProjectID: "my-gcp-project",
                Location:  "us-central1",
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()
    ctx, run := client.StartRun(ctx, "my-job")
    defer run.Finish()

    // Your code here...
}
```

## For Production Use

Once the module is pushed to GitHub with proper tags, users can import it directly:

```bash
go get github.com/OpenLineage/openlineage/client/go@v1.0.0
```

### Versioning for Go Modules in Subdirectories

For Go modules in subdirectories, tags must be prefixed with the subdirectory path:

```bash
# Tag format: <subdirectory>/v<version>
git tag client/go/v1.0.0
git push origin client/go/v1.0.0
```

## Verify the Setup

```bash
# In your project directory, verify the module is accessible
go list -m github.com/OpenLineage/openlineage/client/go

# Build to verify everything compiles
go build ./...

# Run tests
go test ./...
```

## Troubleshooting

### "module not found" errors

Make sure the replace directive path is absolute or correctly relative:

```bash
# Use absolute path
go mod edit -replace github.com/OpenLineage/openlineage/client/go=/absolute/path/to/OpenLineage/client/go
```

### Workspace not picking up changes

```bash
# Sync the workspace
go work sync
```

### Clearing module cache

```bash
go clean -modcache
```
