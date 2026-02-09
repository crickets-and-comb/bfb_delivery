PYTHON_VERSION := 3.12
PACKAGE_NAME := $(shell python -c "import configparser; cfg = configparser.ConfigParser(); cfg.read('setup.cfg'); print(cfg['metadata']['name'])")
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
QC_DIRS := ${REPO_ROOT}src/ ${REPO_ROOT}tests/ ${REPO_ROOT}docs/ ${REPO_ROOT}scripts/

# Enable passing typecheckers
# Both pytype and mypy pass with minimal configuration
# See docs/TYPECHECK_COMPARISON.md for comparison of all 6 tools
RUN_PYTYPE := 1
RUN_MYPY := 1

export
include shared/Makefile

full-test: # Run all the tests.
	$(MAKE) unit

# Overriding to use local shared workflow when running tests, because full-test is overridden and act does not honor that unless the path is relative.
set-CI-CD-file: # Set the CI-CD file to use the local shared CI file.
	perl -pi -e 's|crickets-and-comb/shared/.github/workflows/CI_win\.yml\@main|./shared/.github/workflows/CI.yml|g' .github/workflows/CI_CD_act.yml