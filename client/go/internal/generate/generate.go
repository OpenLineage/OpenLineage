package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if err := facets(); err != nil {
		panic(err)

	}

	if err := openLineage(); err != nil {
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

	cmd := exec.Command(
		"gorename",
		"-from",
		"\"github.com/ThijsKoot/openlineage/client/go/pkg/openlineage\".Run",
		// "\"openlineage\"::Run",
		"-to",
		"RunInfo",
	)
	// cmd := exec.Cmd{
	// 	// Path: "gorename",
	// 	Path: "go",
	// 	Args: []string{
	// 		"run",
	// 		"golang.org/x/tools/cmd/gorename",
	// 	},
	// }
	//
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		fmt.Println(stderr.String())

		return err
	}

	// "encoding/json"::x

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
