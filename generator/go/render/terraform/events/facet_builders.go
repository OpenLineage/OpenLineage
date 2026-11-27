/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package events

import (
	_ "embed"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/OpenLineage/openlineage/generator/go/ir"
)

func RenderFacetBuilders(facets []ir.Facet) string {
	var b strings.Builder

	templateData := struct {
		JobEntries     []facetBuilderEntry
		DatasetEntries []facetBuilderEntry
	}{
		JobEntries:     facetBuilderEntries(facetsByKind(facets, "job")),
		DatasetEntries: facetBuilderEntries(withoutColumnLineage(facetsByKind(facets, "dataset"))),
	}

	if err := facetBuildersTemplate.Execute(&b, templateData); err != nil {
		panic(err)
	}

	return b.String()
}

type facetBuilderEntry struct {
	Facet   string
	Field   string
	PathKey string
}

func facetBuilderEntries(facets []ir.Facet) []facetBuilderEntry {
	out := make([]facetBuilderEntry, 0, len(facets))
	for _, f := range facets {
		out = append(out, facetBuilderEntry{
			Facet:   f.Name,
			Field:   modelFieldName(f.Name),
			PathKey: facetPathKey(f.Name),
		})
	}
	return out
}

func withoutColumnLineage(facets []ir.Facet) []ir.Facet {
	out := make([]ir.Facet, 0, len(facets))
	for _, f := range facets {
		if f.Name == "ColumnLineageDatasetFacet" {
			continue
		}
		out = append(out, f)
	}
	return out
}

func facetsByKind(facets []ir.Facet, kind string) []ir.Facet {
	var out []ir.Facet
	for _, f := range facets {
		if string(f.Kind) == kind {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
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
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := rune(s[i-1])
				nextLower := i+1 < len(s) && unicode.IsLower(rune(s[i+1]))
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

//go:embed facet_builders.tmpl
var facetBuildersTemplateSource string

var facetBuildersTemplate = template.Must(template.New("facet_builders").Parse(facetBuildersTemplateSource))
