/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/OpenLineage/openlineage/byool/terraform/openlineage-base-resource/ol"
)

// datasetModel is the openlineage_dataset state: the base model plus this provider's file_path.
type datasetModel struct {
	ol.DatasetResourceModel
	FilePath types.String `tfsdk:"file_path"`
}

// datasetBackend writes each dataset's latest event to <output_dir>/datasets/<namespace>/<name>.json.
type datasetBackend struct {
	fileBackend
}

var _ ol.DatasetResourceBackend = (*datasetBackend)(nil)

// unsupportedDatasetFacets are left disabled on purpose, to show that configs using them are
// still accepted.
var unsupportedDatasetFacets = []ol.DatasetFacet{ol.CatalogDatasetFacet, ol.FabricOneLakeDatasetFacet}

func supportedDatasetFacets() []ol.DatasetFacet {
	out := []ol.DatasetFacet{ol.ColumnLineageDatasetFacet}
	for _, s := range ol.DatasetFacetSchemas {
		supported := true
		for _, u := range unsupportedDatasetFacets {
			if s.DatasetFacet == u {
				supported = false
			}
		}
		if supported {
			out = append(out, s.DatasetFacet)
		}
	}
	return out
}

// DatasetCapability is what this provider supports for standalone datasets.
func DatasetCapability() ol.DatasetCapability {
	return ol.EmptyDatasetCapability().WithFacetEnabled(supportedDatasetFacets()...)
}

func (b *datasetBackend) Capability() ol.DatasetCapability { return DatasetCapability() }

func (b *datasetBackend) NewModel() any { return &datasetModel{} }

func (b *datasetBackend) ConsumerEmit(_ context.Context, model any) diag.Diagnostics {
	diags := b.configured()
	if diags.HasError() {
		return diags
	}
	m := model.(*datasetModel)
	event := ol.NewDatasetEventBuilder(&diags, b.Capability(), b.cfg.Producer).BuildDatasetEvent(&m.DatasetResourceModel)
	if diags.HasError() {
		return diags
	}
	path := b.eventPath("datasets", m.Namespace.ValueString(), m.Name.ValueString())
	diags.Append(writeEvent(path, event)...)
	m.FilePath = types.StringValue(path)
	return diags
}

func (b *datasetBackend) ConsumerRead(_ context.Context, model any) (bool, diag.Diagnostics) {
	return fileExists(model.(*datasetModel).FilePath.ValueString())
}

func (b *datasetBackend) ConsumerDelete(_ context.Context, model any) diag.Diagnostics {
	return removeFile(model.(*datasetModel).FilePath.ValueString())
}
