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

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
	"github.com/OpenLineage/openlineage/generator/go/render/terraform/tfshape"
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
			Tag:   tfshape.FacetKey(f.Name),
		})
	}

	for _, f := range facetsByKind(facets, discover.FacetDataset) {
		if f.Name == "ColumnLineageDatasetFacet" {
			continue
		}
		templateData.DatasetFields = append(templateData.DatasetFields, facetField{
			Name:  modelFieldName(f.Name),
			Model: f.Name + "Model",
			Tag:   tfshape.FacetKey(f.Name),
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
	base := tfshape.FacetBaseName(facetName)
	switch base {
	case "Datasource":
		return "DataSource"
	case "DatasetVersion":
		return "Version"
	default:
		return base
	}
}

//go:embed resource_models.tmpl
var resourceModelsTemplateSource string

var resourceModelsTemplate = template.Must(template.New("resource_models").Parse(resourceModelsTemplateSource))
