package validator

import (
	"encoding/json"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/santhosh-tekuri/jsonschema/v5/httploader"
	"io"
	"path"
)

var schemaURLs = map[string]string{
	"ErrorMessageRunFacet.json":                "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
	"ExternalQueryRunFacet.json":               "https://openlineage.io/spec/facets/1-0-0/ExternalQueryRunFacet.json",
	"ExtractionErrorRunFacet.json":             "https://openlineage.io/spec/facets/1-0-0/ExtractionErrorRunFacet.json",
	"NominalTimeRunFacet.json":                 "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
	"ParentRunFacet.json":                      "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
	"ProcessingEngineRunFacet.json":            "https://openlineage.io/spec/facets/1-0-0/ProcessingEngineRunFacet.json",
	"DocumentationJobFacet.json":               "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
	"OwnershipJobFacet.json":                   "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
	"SourceCodeJobFacet.json":                  "https://openlineage.io/spec/facets/1-0-0/SourceCodeJobFacet.json",
	"SourceCodeLocationJobFacet.json":          "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
	"SQLJobFacet.json":                         "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
	"ColumnLineageDatasetFacet.json":           "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
	"DatasetVersionDatasetFacet.json":          "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json",
	"DatasourceDatasetFacet.json":              "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
	"DataQualityAssertionsDatasetFacet.json":   "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
	"DocumentationDatasetFacet.json":           "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json",
	"LifecycleStateChangeDatasetFacet.json":    "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
	"OwnershipDatasetFacet.json":               "https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json",
	"SchemaDatasetFacet.json":                  "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
	"StorageDatasetFacet.json":                 "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
	"SymlinksDatasetFacet.json":                "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json",
	"DataQualityMetricsInputDatasetFacet.json": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
	"OutputStatisticsOutputDatasetFacet.json":  "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
}

type Validator struct {
	mainSchema             *jsonschema.Schema
	baseRunFacetSchema     *jsonschema.Schema
	runFacetSchemas        []*jsonschema.Schema
	baseJobFacetSchema     *jsonschema.Schema
	jobFacetSchemas        []*jsonschema.Schema
	baseInputFacetSchema   *jsonschema.Schema
	inputFacetSchemas      []*jsonschema.Schema
	baseOutputFacetSchema  *jsonschema.Schema
	outputFacetSchemas     []*jsonschema.Schema
	baseDatasetFacetSchema *jsonschema.Schema
	datasetFacetSchemas    []*jsonschema.Schema
}

type IEventValidator interface {
	Validate(event string) error
}

func (validator *Validator) Validate(event string) error {
	var doc interface{}
	if err := json.Unmarshal([]byte(event), &doc); err != nil {
		return err
	}

	if err := validator.mainSchema.Validate(doc); err != nil {
		return err
	}

	run := doc.(map[string]interface{})["run"]
	if facets, ok := run.(map[string]interface{})["facets"]; ok {
		for _, facetSchema := range validator.runFacetSchemas {
			if err := facetSchema.Validate(facets); err != nil {
				return err
			}
		}
		for _, facet := range facets.(map[string]interface{}) {
			if err := validator.baseRunFacetSchema.Validate(facet); err != nil {
				return err
			}
		}
	}

	job := doc.(map[string]interface{})["job"]
	if facets, ok := job.(map[string]interface{})["facets"]; ok {
		for _, facetSchema := range validator.jobFacetSchemas {
			if err := facetSchema.Validate(facets); err != nil {
				return err
			}
		}
		for _, facet := range facets.(map[string]interface{}) {
			if err := validator.baseJobFacetSchema.Validate(facet); err != nil {
				return err
			}
		}
	}

	if inputs, ok := doc.(map[string]interface{})["inputs"]; ok {
		for _, input := range inputs.([]interface{}) {
			if facets, ok := input.(map[string]interface{})["inputFacets"]; ok {
				for _, facetSchema := range validator.inputFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := validator.baseInputFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
			if facets, ok := input.(map[string]interface{})["facets"]; ok {
				for _, facetSchema := range validator.datasetFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := validator.baseDatasetFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
		}
	}

	if outputs, ok := doc.(map[string]interface{})["outputs"]; ok {
		for _, output := range outputs.([]interface{}) {
			if facets, ok := output.(map[string]interface{})["outputFacets"]; ok {
				for _, facetSchema := range validator.outputFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := validator.baseOutputFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
			if facets, ok := output.(map[string]interface{})["facets"]; ok {
				for _, facetSchema := range validator.datasetFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := validator.baseDatasetFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func New() *Validator {
	jsonschema.Loaders["https"] = loadSchema

	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat = true

	mainSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json")
	if err != nil {
		panic(err)
	}

	baseRunFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet")
	if err != nil {
		panic(err)
	}

	runFacetSchemas, err := compileRunFacetSchemas(compiler)
	if err != nil {
		panic(err)
	}

	baseJobFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/JobFacet")
	if err != nil {
		panic(err)
	}

	jobFacetSchemas, err := compileJobFacetSchemas(compiler)
	if err != nil {
		panic(err)
	}

	baseInputFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/InputDatasetFacet")
	if err != nil {
		panic(err)
	}

	inputFacetSchemas, err := compileInputFacetSchemas(compiler)
	if err != nil {
		panic(err)
	}

	baseOutputFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/OutputDatasetFacet")
	if err != nil {
		panic(err)
	}

	outputFacetSchemas, err := compileOutputFacetSchemas(compiler)
	if err != nil {
		panic(err)
	}

	baseDatasetFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/DatasetFacet")
	if err != nil {
		panic(err)
	}

	datasetFacetSchemas, err := compileDatasetFacetSchemas(compiler)
	if err != nil {
		panic(err)
	}

	return &Validator{
		mainSchema:             mainSchema,
		baseRunFacetSchema:     baseRunFacetSchema,
		baseJobFacetSchema:     baseJobFacetSchema,
		runFacetSchemas:        runFacetSchemas,
		jobFacetSchemas:        jobFacetSchemas,
		baseInputFacetSchema:   baseInputFacetSchema,
		inputFacetSchemas:      inputFacetSchemas,
		baseOutputFacetSchema:  baseOutputFacetSchema,
		outputFacetSchemas:     outputFacetSchemas,
		baseDatasetFacetSchema: baseDatasetFacetSchema,
		datasetFacetSchemas:    datasetFacetSchemas,
	}
}

func compileRunFacetSchemas(compiler *jsonschema.Compiler) (schemas []*jsonschema.Schema, err error) {
	facets := []string{
		"ErrorMessageRunFacet.json",
		"ExternalQueryRunFacet.json",
		"ExtractionErrorRunFacet.json",
		"NominalTimeRunFacet.json",
		"ParentRunFacet.json",
		"ProcessingEngineRunFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileJobFacetSchemas(compiler *jsonschema.Compiler) (schemas []*jsonschema.Schema, err error) {
	facets := []string{
		"DocumentationJobFacet.json",
		"OwnershipJobFacet.json",
		"SourceCodeJobFacet.json",
		"SourceCodeLocationJobFacet.json",
		"SQLJobFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileInputFacetSchemas(compiler *jsonschema.Compiler) (schemas []*jsonschema.Schema, err error) {
	facets := []string{"DataQualityMetricsInputDatasetFacet.json"}
	return compileFacetSchemas(compiler, facets)
}

func compileOutputFacetSchemas(compiler *jsonschema.Compiler) (schemas []*jsonschema.Schema, err error) {
	facets := []string{"OutputStatisticsOutputDatasetFacet.json"}
	return compileFacetSchemas(compiler, facets)
}

func compileDatasetFacetSchemas(compiler *jsonschema.Compiler) (schemas []*jsonschema.Schema, err error) {
	facets := []string{
		"ColumnLineageDatasetFacet.json",
		"DatasetVersionDatasetFacet.json",
		"DatasourceDatasetFacet.json",
		"DataQualityAssertionsDatasetFacet.json",
		"DocumentationDatasetFacet.json",
		"LifecycleStateChangeDatasetFacet.json",
		"OwnershipDatasetFacet.json",
		"SchemaDatasetFacet.json",
		"StorageDatasetFacet.json",
		"SymlinksDatasetFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileFacetSchemas(compiler *jsonschema.Compiler, facets []string) (schemas []*jsonschema.Schema, err error) {
	schemas = make([]*jsonschema.Schema, len(facets))
	for i, schemaKey := range facets {
		schemaURL := schemaURLs[schemaKey]
		schemas[i], err = compiler.Compile(schemaURL)
		if err != nil {
			return
		}
	}
	return
}

func loadSchema(url string) (io.ReadCloser, error) {
	if schemaURL, ok := schemaURLs[path.Base(url)]; ok {
		return httploader.Load(schemaURL)
	} else {
		return httploader.Load(url)
	}
}
