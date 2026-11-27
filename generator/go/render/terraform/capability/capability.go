/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package capability

import (
	_ "embed"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenLineage/openlineage/generator/go/discover"
	"github.com/OpenLineage/openlineage/generator/go/ir"
)

func RenderCapabilityFile(facets []ir.Facet) string {
	var b strings.Builder

	templateData := struct {
		JobFacetNames     []string
		DatasetFacetNames []string
	}{
		JobFacetNames:     collectFacetNames(facets, "job"),
		DatasetFacetNames: collectFacetNames(facets, "dataset"),
	}

	if err := capabilityBodyTemplate.Execute(&b, templateData); err != nil {
		panic(err)
	}

	return b.String()
}

// Collect, deduplicate, and sort facet names deterministically
func collectFacetNames(facets []ir.Facet, kind discover.FacetType) []string {
	seen := map[string]bool{}
	var names []string

	for _, f := range facets {
		if f.Kind != kind {
			continue
		}
		if seen[f.Name] {
			continue
		}
		seen[f.Name] = true
		names = append(names, f.Name)
	}

	sort.Strings(names)
	return names
}

//go:embed capability_body.tmpl
var capabilityBodyTemplateSource string

var capabilityBodyTemplate = template.Must(template.New("capability_body").Parse(capabilityBodyTemplateSource))
