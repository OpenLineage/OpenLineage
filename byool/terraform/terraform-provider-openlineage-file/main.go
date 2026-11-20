/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

// Command terraform-provider-openlineage is an example Terraform provider built on the
// OpenLineage base resource. It writes the latest OpenLineage event of each job and dataset
// to a JSON file.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/OpenLineage/openlineage/byool/terraform/terraform-provider-openlineage-file/internal/provider"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "run the provider with support for debuggers like delve")
	flag.Parse()

	err := providerserver.Serve(context.Background(), provider.New(version), providerserver.ServeOpts{
		Address: "registry.terraform.io/openlineage/openlineage",
		Debug:   debug,
	})
	if err != nil {
		log.Fatal(err)
	}
}
