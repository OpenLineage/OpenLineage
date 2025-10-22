# OpenLineage Development Makefile
# 
# This project uses path-based dependencies instead of a UV workspace
# Each integration is now a standalone project with isolated dependencies

.PHONY: help setup-* test-* lint-* clean

# Colors for output
BLUE := \033[34m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)OpenLineage Development Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-25s$(NC) %s\n", $$1, $$2}'

# =============================================================================
# Setup Commands - Install dependencies for specific integrations
# =============================================================================

setup-client: ## Setup Python client
	@echo "$(BLUE)Setting up Python client...$(NC)"
	cd client/python && uv sync --extra test --extra dev --extra generator --extra kafka --extra msk-iam --extra datazone --extra fsspec --active

setup-common: ## Setup integration common library
	@echo "$(BLUE)Setting up integration common...$(NC)"
	cd integration/common && uv sync --extra dev --active

setup-airflow: ## Setup Airflow integration (with airflow-compatible deps)
	@echo "$(BLUE)Setting up Airflow integration...$(NC)"
	cd integration/airflow && uv sync --extra airflow --extra dev  --active

setup-dbt: ## Setup dbt integration
	@echo "$(BLUE)Setting up dbt integration...$(NC)"
	cd integration/dbt && uv sync --extra dev --active

# =============================================================================
# Testing Commands
# =============================================================================

test-all: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	@$(MAKE) test-client
	@$(MAKE) test-common
	@$(MAKE) test-airflow
	@$(MAKE) test-dbt
	@echo "$(GREEN)✅ All tests completed!$(NC)"

test-client: ## Test Python client
	@echo "$(BLUE)Testing Python client...$(NC)"
	@$(MAKE) setup-client
	cd client/python && uv run pytest tests/

test-common: ## Test integration common library
	@echo "$(BLUE)Testing integration common...$(NC)"
	@$(MAKE) setup-common
	cd integration/common && uv run pytest tests/

test-airflow: ## Test Airflow integration
	@echo "$(BLUE)Testing Airflow integration...$(NC)"
	@$(MAKE) setup-airflow
	cd integration/airflow && uv run pytest -vv tests/

test-dbt: ## Test dbt integration
	@echo "$(BLUE)Testing dbt integration...$(NC)"
	@$(MAKE) setup-dbt
	cd integration/dbt && uv run pytest tests/

# =============================================================================
# Linting & Formatting
# =============================================================================

lint-all: ## Run all linting and type checking
	@echo "$(BLUE)Running linting and type checking...$(NC)"
	@$(MAKE) lint-format
	@$(MAKE) lint-types
	@echo "$(GREEN)✅ All linting completed!$(NC)"

lint-format: ## Run ruff formatting and linting
	@echo "$(BLUE)Running ruff checks...$(NC)"
	uv tool run ruff check .
	uv tool run ruff format --check .

lint-types: ## Run mypy type checking per integration
	@echo "$(BLUE)Running mypy type checking...$(NC)"
	cd client/python && uv run mypy src/
	cd integration/common && uv run mypy src/
	cd integration/airflow && uv run mypy src/ --ignore-missing-imports
	cd integration/dbt && uv run mypy src/ --ignore-missing-imports

fix-format: ## Auto-fix formatting issues
	@echo "$(BLUE)Auto-fixing format issues...$(NC)"
	uv tool run ruff format .
	uv tool run ruff check --fix .

# =============================================================================
# Development Shortcuts
# =============================================================================

airflow: ## Enter Airflow integration directory
	@echo "$(GREEN)💨 Switching to Airflow integration$(NC)"
	@echo "Run: cd integration/airflow && uv sync --extra airflow --extra tests"
	@cd integration/airflow && bash

dbt: ## Enter dbt integration directory
	@echo "$(GREEN)🔧 Switching to dbt integration$(NC)"
	@echo "Run: cd integration/dbt && uv sync --extra tests"
	@cd integration/dbt && bash

client: ## Enter Python client directory
	@echo "$(GREEN)🐍 Switching to Python client$(NC)"
	@echo "Run: cd client/python && uv sync --extra tests"
	@cd client/python && bash

# =============================================================================
# Status & Information
# =============================================================================

status: ## Show status of all integrations
	@echo "$(BLUE)Integration Status:$(NC)"
	@echo -n "Client Python: "; \
	if [ -d "client/python/.venv" ]; then echo "$(GREEN)✅ Ready$(NC)"; else echo "$(RED)❌ Not setup$(NC)"; fi
	@echo -n "Common: "; \
	if [ -d "integration/common/.venv" ]; then echo "$(GREEN)✅ Ready$(NC)"; else echo "$(RED)❌ Not setup$(NC)"; fi
	@echo -n "Airflow: "; \
	if [ -d "integration/airflow/.venv" ]; then echo "$(GREEN)✅ Ready$(NC)"; else echo "$(RED)❌ Not setup$(NC)"; fi
	@echo -n "dbt: "; \
	if [ -d "integration/dbt/.venv" ]; then echo "$(GREEN)✅ Ready$(NC)"; else echo "$(RED)❌ Not setup$(NC)"; fi

deps: ## Show dependency versions (for debugging conflicts)
	@echo "$(BLUE)Dependency Versions:$(NC)"
	@echo "$(YELLOW)Airflow protobuf:$(NC)"
	@cd integration/airflow && uv run python -c "import protobuf; print(f'  protobuf: {protobuf.__version__}')" 2>/dev/null || echo "  Not installed"

# =============================================================================
# Utility Commands
# =============================================================================

clean: ## Clean all virtual environments and caches
	@echo "$(YELLOW)Cleaning virtual environments and caches...$(NC)"
	rm -rf client/python/.venv
	rm -rf integration/common/.venv
	rm -rf integration/airflow/.venv
	rm -rf integration/dbt/.venv
	uv cache clean
	@echo "$(GREEN)✅ Cleanup completed!$(NC)"

# =============================================================================
# CI Simulation
# =============================================================================

ci-test: ## Run the same checks that CI runs
	@echo "$(BLUE)Running CI simulation...$(NC)"
	@$(MAKE) lint-all
	@$(MAKE) test-all
	@echo "$(GREEN)✅ All CI checks passed!$(NC)"