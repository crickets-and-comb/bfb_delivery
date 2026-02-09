PYTHON_VERSION := 3.12
PACKAGE_NAME := $(shell python -c "import configparser; cfg = configparser.ConfigParser(); cfg.read('setup.cfg'); print(cfg['metadata']['name'])")
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
QC_DIRS := ${REPO_ROOT}src/ ${REPO_ROOT}tests/ ${REPO_ROOT}docs/ ${REPO_ROOT}scripts/

# Typechecker selection: using pytype (passes with 0 errors)
# Other tools tested but disabled due to requiring substantial code refactoring:
# - mypy: 91 errors (58 in src/)
# - pyright: 194 errors (79 in src/)
# - basedpyright: 230 errors + 942 warnings
# See docs/TYPECHECK_COMPARISON.md for detailed comparison and recommendation.

export
include shared/Makefile

full-test: # Run all the tests.
	$(MAKE) unit

# Overriding to use local shared workflow when running tests, because full-test is overridden and act does not honor that unless the path is relative.
set-CI-CD-file: # Set the CI-CD file to use the local shared CI file.
	perl -pi -e 's|crickets-and-comb/shared/.github/workflows/CI_win\.yml\@main|./shared/.github/workflows/CI.yml|g' .github/workflows/CI_CD_act.yml