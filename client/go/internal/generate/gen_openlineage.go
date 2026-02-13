package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os/exec"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
)

func generateOpenLineageCode() (string, error) {
	quicktypeCommand := strings.Join([]string{
		"quicktype",
		"-l",
		"go",
		"--src-lang",
		"schema",
		"--package",
		"openlineage",
		"--top-level",
		"Event",
		"--just-types-and-package",
		"--no-ignore-json-refs",
		"../../spec/OpenLineage.json",
	}, " ")

	args := []string{
		"-c",
		quicktypeCommand,
	}

	cmd := exec.Command("bash", args...)
	result, err := cmd.Output()
	if err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			fmt.Println(string(exitError.Stderr))

		}
		return "", err
	}

	code := string(result)

	replacements := map[string]string{
		"package openlineage":         "package openlineage\n\nimport \"github.com/OpenLineage/openlineage/client/go/pkg/facets\"\n",
		"map[string]InputFacetValue":  "*facets.InputDatasetFacets",
		"map[string]DatasetFacet":     "*facets.DatasetFacets",
		"map[string]JobFacet":         "*facets.JobFacets",
		"map[string]OutputFacetValue": "*facets.OutputDatasetFacets",
		"map[string]RunFacet":         "*facets.RunFacets",
		"Abort":                       "EventTypeAbort",
		"Fail":                        "EventTypeFail",
		"Complete":                    "EventTypeComplete",
		"Other":                       "EventTypeOther",
		"Running":                     "EventTypeRunning",
		"Start":                       "EventTypeStart",
	}

	for k, v := range replacements {
		code = strings.ReplaceAll(code, k, v)
	}

	return code, nil
}

func removeFacetBaseTypes(code string) (string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "facets.gen.go", code, parser.ParseComments)
	if err != nil {
		return "", err
	}

	result := astutil.Apply(file, nil, func(c *astutil.Cursor) bool {
		n := c.Node()
		switch x := n.(type) {
		case *ast.GenDecl:
			if x.Tok != token.TYPE {
				return true
			}

			spec := x.Specs[0].(*ast.TypeSpec)
			typeDeclName := spec.Name.String()

			deletions := []string{
				"InputFacetValue",
				"DatasetFacet",
				"JobFacet",
				"OutputFacetValue",
				"RunFacet",
			}

			for _, d := range deletions {
				if typeDeclName == d {
					c.Delete()
					return true
				}
			}
		}

		return true
	})

	var out bytes.Buffer
	if err := format.Node(&out, fset, result); err != nil {
		return "", err
	}

	return out.String(), nil
}
