PACKAGE_NAME := $(shell python -c "import configparser; cfg = configparser.ConfigParser(); cfg.read('setup.cfg'); print(cfg['metadata']['name'])")
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
QC_DIRS := ${REPO_ROOT}src/ ${REPO_ROOT}tests/ ${REPO_ROOT}docs/ ${REPO_ROOT}scripts/

# Enable mypy (passes with minimal configuration)
# pytype is enabled by default on Python 3.12 (see shared Makefile)
# PYTHON_VERSION is handled by shared Makefile (defaults to 3.12, overridden by CI)
# See docs/TYPECHECK_COMPARISON.md for comparison of all 6 tools
RUN_MYPY := 1

export
include shared/Makefile

full-test: # Run all the tests.
	$(MAKE) unit

# Overriding to use local shared workflow when running tests, because full-test is overridden and act does not honor that unless the path is relative.
set-CI-CD-file: # Set the CI-CD file to use the local shared CI file.
	perl -pi -e 's|crickets-and-comb/shared/.github/workflows/CI_win\.yml\@main|./shared/.github/workflows/CI.yml|g' .github/workflows/CI_CD_act.yml