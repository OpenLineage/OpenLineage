package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"github.com/iancoleman/strcase"
	"golang.org/x/tools/go/ast/astutil"

	_ "embed"
)

var (
	//go:embed facet_helpers.go.tmpl
	facetHelperTemplate string

	schemaURLs map[string]string
)

type facetKind string

const (
	facetTypeInputDataset  = "InputDatasetFacet"
	facetTypeOutputDataset = "OutputDatasetFacet"
	facetTypeDataset       = "DatasetFacet"
	facetTypeJob           = "JobFacet"
	facetTypeRun           = "RunFacet"
)

type facetSpec struct {
	Name           string
	Tag            string
	Fields         []facetFieldSpec
	OptionalFields []facetFieldSpec
	Kind           facetKind
	Producer       string
	SchemaURL      string
}

type facetFieldSpec struct {
	Name      string
	ParamName string
	Typ       string
	IsRefType bool
}

func generateFacets() (string, error) {
	quicktypeCommand := strings.Join([]string{
		"quicktype",
		"-l",
		"go",
		"--src-lang",
		"schema",
		"--package",
		"facets",
		"--just-types-and-package",
		"--no-ignore-json-refs",
		"../../spec/facets/*.json",
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
		"OwnershipDatasetFacetOwnership":         "DatasetOwnership",
		"PurpleOwner":                            "DatasetOwner",
		"OwnershipJobFacetOwnership":             "JobOwnership",
		"FluffyOwner":                            "JobOwner",
		"DocumentationDatasetFacetDocumentation": "DatasetDocumentation",
		// "NewDocumentationJobFacetDocumentation":  "JobDocumentation",
		"DocumentationJobFacetDocumentation": "JobDocumentation",
	}

	for k, v := range replacements {
		code = strings.ReplaceAll(code, k, v)
	}

	return code, nil
}

func extractFacets(code string) ([]facetSpec, error) {
	// Create a FileSet to work with
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "facets.gen.go", code, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var facets []facetSpec

	ast.Inspect(file, func(n ast.Node) bool {
		// Find Function Call Statements
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		wrapperName := typeSpec.Name.String()
		if !strings.HasSuffix(wrapperName, "Facet") {
			return true
		}

		kind, err := deduceFacetKind(wrapperName)
		if err != nil {
			return true
		}

		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return true
		}

		if len(structType.Fields.List) != 1 {
			return false
		}

		// fields := typeSpec.Doc
		facetField := structType.Fields.List[0]

		facetIdent := facetField.Type.(*ast.StarExpr).X.(*ast.Ident)
		// facetPointer, ok := facetField.Type.(*ast.StarExpr).X.(*ast.Ident)
		// if !ok {
		// 	panic(1)
		// }
		//
		// facetIdent, ok := facetPointer.X.(*ast.Ident)
		// if !ok {
		// 	panic(1)
		// }

		name := facetIdent.Name
		schemaURL, err := getSchemaURL(name, kind)
		if err != nil {
			panic(err)
		}
		// schemaURL, ok := schemaURLs[name]
		// if !ok {
		// 	log.Fatalf("schema url not found: %s", name)
		// }

		facet := facetSpec{
			Tag:  facetField.Tag.Value,
			Name: name,
			// Name:      facetField.Names[0].Name,
			Kind:      kind,
			Producer:  "openlineage-go",
			SchemaURL: schemaURL,
		}

		facetTypeSpec, ok := facetIdent.Obj.Decl.(*ast.TypeSpec)
		if !ok {
			panic(1)
		}

		facetStruct, ok := facetTypeSpec.Type.(*ast.StructType)
		if !ok {
			panic(1)
		}

		for _, f := range facetStruct.Fields.List {
			fName := f.Names[0].String()
			if fName == "SchemaURL" || fName == "Producer" {
				continue
			}

			var fType string

			var optional bool
			var isRefType bool

			switch x := f.Type.(type) {
			case *ast.StarExpr:
				// elem := x.X.(*ast.Ident).Name
				// fType = fmt.Sprintf("*%s", elem)

				fType = x.X.(*ast.Ident).Name
				optional = true
			case *ast.Ident:
				fType = x.Name
			case *ast.ArrayType:
				elem := x.Elt.(*ast.Ident).Obj.Name
				fType = fmt.Sprintf("[]%s", elem)
				optional = true
				isRefType = true
			case *ast.MapType:
				mt := x
				k := mt.Key.(*ast.Ident).Name
				v := mt.Value.(*ast.Ident).Name
				optional = true
				isRefType = true

				fType = fmt.Sprintf("map[%s]%s", k, v)
			}

			paramName := strcase.ToLowerCamel(fName)
			if paramName == "type" {
				paramName = "typ"
			}

			field := facetFieldSpec{
				Name:      fName,
				ParamName: paramName,
				Typ:       fType,
				IsRefType: isRefType,
			}

			if optional {
				facet.OptionalFields = append(facet.OptionalFields, field)
			} else {
				facet.Fields = append(facet.Fields, field)
			}
		}

		facets = append(facets, facet)

		return true
	})

	return facets, nil
}

func generateFacetHelpers(facets []facetSpec) (string, error) {
	t := template.New("main")
	tmpl, err := t.Parse(facetHelperTemplate)
	if err != nil {
		return "", err
	}

	data := map[string]any{
		"facets": facets,
		"facetKinds": []string{
			facetTypeInputDataset,
			facetTypeOutputDataset,
			facetTypeDataset,
			facetTypeJob,
			facetTypeRun,
		},
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func deduceFacetKind(name string) (facetKind, error) {
	kinds := []facetKind{
		facetTypeInputDataset,
		facetTypeOutputDataset,
		facetTypeDataset,
		facetTypeJob,
		facetTypeRun,
	}

	for _, k := range kinds {
		if strings.HasSuffix(name, string(k)) {
			return k, nil
		}
	}

	return "", fmt.Errorf("can't deduce facetKind from %s", name)
}

func removeFacetWrappers(code string) (string, error) {
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
			if strings.HasSuffix(typeDeclName, "Facet") {
				c.Delete()
				return true
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

// getSchemaURL reads the JSONSchema for a facet and returns its $id
func getSchemaURL(facetName string, facetKind facetKind) (string, error) {
	replacements := map[string]string{
		// Not sure why this facet ends up being called Version instead of DatasetVersion
		"Version": "DatasetVersion",
		// Inconsistency in facet filename
		"DataSource":           "Datasource",
		"DatasetDocumentation": "Documentation",
		"JobDocumentation":     "Documentation",
		"DatasetOwnership":     "Ownership",
		"JobOwnership":         "Ownership",
	}

	for k, v := range replacements {
		if facetName == k {
			facetName = v
			break
		}
	}

	filepath := fmt.Sprintf("../../spec/facets/%s%s.json", facetName, facetKind)
	f, err := os.ReadFile(filepath)
	if err != nil {
		return "", err
	}

	var schema map[string]any
	if err := json.Unmarshal(f, &schema); err != nil {
		return "", err
	}

	id, ok := schema["$id"]
	if !ok {
		return "", errors.New("$id field not found")
	}

	return id.(string), nil
}
