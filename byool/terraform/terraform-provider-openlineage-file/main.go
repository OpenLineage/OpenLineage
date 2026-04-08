/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/OpenLineage/openlineage/byool/terraform/terraform-provider-openlineage-file/internal/file"
)

// Run "go generate" to format example terraform files and regenerate docs.
//
//go:generate terraform fmt -recursive ./examples/
//go:generate go run github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs

var (
	// version is set by GoReleaser at build time; "dev" during local development.
	version string = "dev"
)

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "enable debug mode for use with delve / DAP")
	flag.Parse()

	opts := providerserver.ServeOpts{
		// Registry address follows the standard Terraform convention:
		//   registry.terraform.io/<namespace>/<type>
		// where the binary is named terraform-provider-<type>.
		Address: "registry.terraform.io/openlineage/openlineage-file",
		Debug:   debug,
	}

	if err := providerserver.Serve(context.Background(), file.New(version), opts); err != nil {
		log.Fatal(err.Error())
	}
}
