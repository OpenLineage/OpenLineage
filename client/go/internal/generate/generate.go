/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	if err := facets(); err != nil {
		fmt.Fprintf(os.Stderr, "facets() failed: %v\n", err)
		panic(err)

	}

	if err := openLineage(); err != nil {
		fmt.Fprintf(os.Stderr, "openLineage() failed: %v\n", err)
		panic(err)

	}
}

func openLineage() error {
	baseCode, err := generateOpenLineageCode()
	if err != nil {
		return err
	}

	edited, err := removeFacetBaseTypes(baseCode)
	if err != nil {
		return err
	}

	file, err := os.Create("pkg/openlineage/spec.gen.go")
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(edited); err != nil {
		return err
	}


	return nil
}

func facets() error {
	baseFacetCode, err := generateFacets()
	if err != nil {
		return err
	}

	facets, err := extractFacets(baseFacetCode)
	if err != nil {
		return err
	}

	facetHelperCode, err := generateFacetHelpers(facets)
	if err != nil {
		return err
	}

	editedFacetCode, err := removeFacetWrappers(baseFacetCode)
	if err != nil {
		return err
	}

	facetFile, err := os.Create("pkg/facets/facets.gen.go")
	if err != nil {
		return err
	}
	defer facetFile.Close()

	if _, err := facetFile.WriteString(editedFacetCode); err != nil {
		return err
	}

	facetHelpersFile, err := os.Create("pkg/facets/facet_helpers.gen.go")
	if err != nil {
		return err
	}
	defer facetHelpersFile.Close()

	if _, err := facetHelpersFile.WriteString(facetHelperCode); err != nil {
		return err
	}

	return nil
}
