BINARY_NAME  = terraform-provider-openlineage-file
INSTALL_DIR  = bin
OS_ARCH      = $(shell go env GOOS)_$(shell go env GOARCH)
TF_ENV       = TF_CLI_CONFIG_FILE=$(PWD)/.terraformrc
GOPATH_BIN   = $(shell go env GOPATH)/bin

.PHONY: build example plan apply destroy clean test docs docs-install

# Generate terraform-example/main.tf by concatenating the committed example files.
# The generated file is git-ignored — edit the source files under examples/ instead.
example:
	@mkdir -p terraform-example
	@cat \
	  examples/provider/provider.tf \
	  examples/resources/openlineage_run/resource.tf \
	  examples/resources/openlineage_job/resource.tf \
	  examples/resources/openlineage_dataset/resource.tf \
	  > terraform-example/main.tf
	@echo "✅ terraform-example/main.tf generated from examples/"

# Build the provider binary into bin/.
# The .terraformrc dev_overrides block points Terraform to this directory,
# so no `terraform init` is needed for local development.
build:
	mkdir -p $(INSTALL_DIR)
	go build -o $(INSTALL_DIR)/$(BINARY_NAME) .

# Run terraform plan with dev_overrides (no init needed).
plan: build example
	@echo "Using dev_overrides — 'terraform init' is not required."
	cd terraform-example && $(TF_ENV) terraform plan

# Apply the example configuration.
apply: build example
	cd terraform-example && $(TF_ENV) terraform apply -auto-approve

# Destroy all managed resources and remove the lineage files.
destroy: build example
	cd terraform-example && $(TF_ENV) terraform destroy -auto-approve

# Remove build artifacts and terraform state.
clean:
	rm -rf $(INSTALL_DIR)
	rm -rf terraform-example/lineage
	rm -f  terraform-example/terraform.tfstate terraform-example/terraform.tfstate.backup
	rm -rf terraform-example/.terraform

# Run all tests.
test:
	go test ./... -v

# Generate provider documentation from schema + templates.
docs:
	$(GOPATH_BIN)/tfplugindocs generate --provider-name openlineage
	@echo "✅ Docs generated in docs/"

# Install tfplugindocs if not present.
docs-install:
	go install github.com/hashicorp/terraform-plugin-docs/cmd/tfplugindocs@latest

