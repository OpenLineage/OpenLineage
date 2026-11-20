/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
)

// fileBackend holds what job and dataset backends share: the provider config and the
// computed file_path attribute.
type fileBackend struct {
	cfg *Config
}

func (b *fileBackend) ConsumerConfigure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return // not configured yet (validation)
	}
	cfg, ok := req.ProviderData.(*Config)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", fmt.Sprintf("got %T, want *provider.Config", req.ProviderData))
		return
	}
	b.cfg = cfg
}

func (b *fileBackend) ConsumerAttributes() map[string]schema.Attribute {
	return map[string]schema.Attribute{
		"file_path": schema.StringAttribute{
			Computed:    true,
			Description: "File the latest event is written to.",
			// The path depends only on namespace and name, which replace the resource.
			PlanModifiers: []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
		},
	}
}

func (b *fileBackend) ConsumerBlocks() map[string]schema.Block {
	return nil
}

// eventPath returns <output_dir>/<kind>/<namespace>/<name>.json with each segment escaped,
// so namespaces like s3://bucket stay one directory.
func (b *fileBackend) eventPath(kind, namespace, name string) string {
	return filepath.Join(b.cfg.OutputDir, kind, segment(namespace), segment(name)+".json")
}

func segment(s string) string {
	switch s {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	}
	return url.PathEscape(s)
}

func (b *fileBackend) configured() diag.Diagnostics {
	var diags diag.Diagnostics
	if b.cfg == nil {
		diags.AddError("Provider not configured", "The openlineage provider must be configured before resources are applied.")
	}
	return diags
}

func writeEvent(path string, event any) diag.Diagnostics {
	var diags diag.Diagnostics
	data, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		diags.AddError("Encode event", err.Error())
		return diags
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		diags.AddError("Create event directory", err.Error())
		return diags
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		diags.AddError("Write event file", err.Error())
	}
	return diags
}

func fileExists(path string) (bool, diag.Diagnostics) {
	var diags diag.Diagnostics
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, diags
	case errors.Is(err, os.ErrNotExist):
		return false, diags
	}
	diags.AddError("Read event file", err.Error())
	return false, diags
}

func removeFile(path string) diag.Diagnostics {
	var diags diag.Diagnostics
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		diags.AddError("Delete event file", err.Error())
	}
	return diags
}
