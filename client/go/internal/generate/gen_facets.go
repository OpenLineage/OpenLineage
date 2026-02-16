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
	JSONName       string
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

	// Consolidate imports: quicktype scatters imports throughout the file
	code = consolidateImports(code)


	return code, nil
}

// consolidateImports finds all import statements scattered throughout the code,
// removes them from their original positions, and adds a single import block
// at the top of the file after the package declaration.
func consolidateImports(code string) string {
	lines := strings.Split(code, "\n")
	if len(lines) == 0 {
		return code
	}

	// First line is always the package declaration
	packageLine := lines[0]

	// Collect unique imports and filter out import lines from the rest
	imports := make(map[string]bool)
	var codeLines []string

	for _, line := range lines[1:] {
		trimmed := strings.TrimSpace(line)

		// Check for single-line import: import "time"
		if strings.HasPrefix(trimmed, "import \"") {
			start := strings.Index(trimmed, "\"")
			end := strings.LastIndex(trimmed, "\"")
			if start != -1 && end > start {
				pkg := trimmed[start+1 : end]
				imports[pkg] = true
			}
			continue // Skip import lines
		}

		codeLines = append(codeLines, line)
	}

	// Build output: package + imports + code
	var output strings.Builder
	output.WriteString(packageLine)
	output.WriteString("\n")

	if len(imports) > 0 {
		output.WriteString("\nimport (\n")
		for pkg := range imports {
			output.WriteString(fmt.Sprintf("\t\"%s\"\n", pkg))
		}
		output.WriteString(")\n")
	}

	output.WriteString(strings.Join(codeLines, "\n"))

	return output.String()
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

		// Handle both pointer (*Type) and non-pointer (Type) field types
		var facetIdent *ast.Ident
		switch fieldType := facetField.Type.(type) {
		case *ast.StarExpr:
			// Pointer type: *SomeFacet
			facetIdent = fieldType.X.(*ast.Ident)
		case *ast.Ident:
			// Non-pointer type: SomeFacet
			facetIdent = fieldType
		default:
			// Unknown type, skip this facet
			return true
		}

		name := facetIdent.Name
		schemaURL, err := getSchemaURL(wrapperName)
		if err != nil {
			panic(err)
		}
		// schemaURL, ok := schemaURLs[name]
		// if !ok {
		// 	log.Fatalf("schema url not found: %s", name)
		// }

		// Extract JSON field name from tag (e.g., `json:"subset,omitempty"` -> "subset")
		jsonName := extractJSONName(facetField.Tag.Value)

		facet := facetSpec{
			Tag:      facetField.Tag.Value,
			Name:     name,
			JSONName: jsonName,
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
				// Handle pointer types like *string, *int64
				switch inner := x.X.(type) {
				case *ast.Ident:
					fType = inner.Name
				case *ast.SelectorExpr:
					// *time.Time -> time.Time
					fType = fmt.Sprintf("%s.%s", inner.X.(*ast.Ident).Name, inner.Sel.Name)
				default:
					continue // Skip unsupported types
				}
				optional = true
			case *ast.Ident:
				fType = x.Name
			case *ast.SelectorExpr:
				// time.Time
				fType = fmt.Sprintf("%s.%s", x.X.(*ast.Ident).Name, x.Sel.Name)
			case *ast.ArrayType:
				switch elem := x.Elt.(type) {
				case *ast.Ident:
					fType = fmt.Sprintf("[]%s", elem.Name)
				case *ast.SelectorExpr:
					fType = fmt.Sprintf("[]%s.%s", elem.X.(*ast.Ident).Name, elem.Sel.Name)
				default:
					continue // Skip unsupported types
				}
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
// wrapperName is the full wrapper type name (e.g., "ErrorMessageRunFacet")
func getSchemaURL(wrapperName string) (string, error) {
	filepath := fmt.Sprintf("../../spec/facets/%s.json", wrapperName)
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

// extractJSONName extracts the JSON field name from a struct tag
// e.g., `json:"subset,omitempty"` -> "subset"
func extractJSONName(tag string) string {
	// Remove backticks
	tag = strings.Trim(tag, "`")

	// Find json:"..."
	const prefix = `json:"`
	start := strings.Index(tag, prefix)
	if start == -1 {
		return ""
	}
	start += len(prefix)

	// Find closing quote
	end := strings.Index(tag[start:], `"`)
	if end == -1 {
		return ""
	}

	jsonTag := tag[start : start+end]

	// Handle "name,omitempty" -> "name"
	if comma := strings.Index(jsonTag, ","); comma != -1 {
		return jsonTag[:comma]
	}

	return jsonTag
}

