/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package model

import (
	_ "embed"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
)

func RenderResourceModels(facets []ir.Facet) string {
	var b strings.Builder

	templateData := struct {
		HasColumnLineage bool
		JobFacetFields   []facetField
		DatasetFields    []facetField
	}{
		HasColumnLineage: hasFacet(facets, "ColumnLineageDatasetFacet"),
	}

	for _, f := range facetsByKind(facets, discover.FacetJob) {
		templateData.JobFacetFields = append(templateData.JobFacetFields, facetField{
			Name:  modelFieldName(f.Name),
			Model: f.Name + "Model",
			Tag:   facetPathKey(f.Name),
		})
	}

	for _, f := range facetsByKind(facets, discover.FacetDataset) {
		if f.Name == "ColumnLineageDatasetFacet" {
			continue
		}
		templateData.DatasetFields = append(templateData.DatasetFields, facetField{
			Name:  modelFieldName(f.Name),
			Model: f.Name + "Model",
			Tag:   facetPathKey(f.Name),
		})
	}

	if err := resourceModelsTemplate.Execute(&b, templateData); err != nil {
		panic(err)
	}

	return b.String()
}

type facetField struct {
	Name  string
	Model string
	Tag   string
}

func facetsByKind(facets []ir.Facet, kind discover.FacetType) []ir.Facet {
	out := make([]ir.Facet, 0)
	for _, f := range facets {
		if f.Kind == kind {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func hasFacet(facets []ir.Facet, name string) bool {
	for _, f := range facets {
		if f.Name == name {
			return true
		}
	}
	return false
}

func modelFieldName(facetName string) string {
	base := facetBaseName(facetName)
	switch base {
	case "Datasource":
		return "DataSource"
	case "DatasetVersion":
		return "Version"
	default:
		return base
	}
}

func facetPathKey(facetName string) string {
	base := facetBaseName(facetName)
	if base == "DatasetVersion" {
		return "version"
	}
	return toSnakeCase(base)
}

func facetBaseName(facetName string) string {
	base := strings.TrimSuffix(facetName, "Facet")
	base = strings.TrimSuffix(base, "Job")
	base = strings.TrimSuffix(base, "Dataset")
	return base
}

func toSnakeCase(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || nextLower {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

//go:embed resource_models.tmpl
var resourceModelsTemplateSource string

var resourceModelsTemplate = template.Must(template.New("resource_models").Parse(resourceModelsTemplateSource))
