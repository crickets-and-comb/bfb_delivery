PYTHON_VERSION := 3.12
PACKAGE_NAME := $(shell python -c "import configparser; cfg = configparser.ConfigParser(); cfg.read('setup.cfg'); print(cfg['metadata']['name'])")
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
QC_DIRS := ${REPO_ROOT}src/ ${REPO_ROOT}tests/ ${REPO_ROOT}docs/ ${REPO_ROOT}scripts/

export
include shared/Makefile

full-test: # Run all the tests. (NOTE: this means running run-act requires switching the path to the shared CI workflow to a relative path in CI_CD.yml.)
	$(MAKE) unit