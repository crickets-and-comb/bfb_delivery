PYTHON_VERSION = 3.12
PACKAGE_NAME = $(shell python -c "import configparser; cfg = configparser.ConfigParser(); cfg.read('setup.cfg'); print(cfg['metadata']['name'])")
CONDA_ENV_NAME = ${PACKAGE_NAME}_py${PYTHON_VERSION}
REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

IGNORE_SAFETY = y
IGNORE_JAKE = y
VULNERABILITIES_ENGINE = safety
QC_DIRS = ${REPO_ROOT}/src ${REPO_ROOT}/tests ${REPO_ROOT}/docs
INSTALL_EXTRAS := # [build] [dev] [qc] [test] [doc]
USE_LOCKFILE = n
SKIP_LOCKFILE_INSTALL = n
DIST_DIR = dist

ACT_RUN_EVENT := workflow_dispatch
MATRIX_OS := ubuntu-latest
MATRIX_PYTHON_VERSION :=

.DEFAULT_GOAL = list-targets
this_makefile = $(lastword $(MAKEFILE_LIST))


list-targets: # Print make targets.
	@echo "Makefile targets:"
	@grep -i "^[a-zA-Z][a-zA-Z0-9_ \.\-]*: .*[#].*" ${this_makefile} | sort | sed 's/:.*#/ : /g' | column -t -s:

build-env: # Build the dev env. You may want to add other extras here like mysqlclient etc. This does not install the package under development.
	conda create -c defaults --override-channels -n ${CONDA_ENV_NAME} python=${PYTHON_VERSION} --yes

remove-env: # Remove the dev env.
	conda env remove --n ${CONDA_ENV_NAME}

install: # Install this package in local editable mode, and install dev dependencies.
ifeq ("${USE_LOCKFILE}", "y")
	$(MAKE) install-lockfile
else
	python -m pip install --upgrade pip setuptools
	python -m pip install -e ${REPO_ROOT}${INSTALL_EXTRAS}
endif

install-lockfile: # Install this package in editable mode, and generate and/or use lockfile to specify dependencies.
	pip install --upgrade pip setuptools
	@if [ ! -f "${REPO_ROOT}/pip.lock" ]; then \
		pip install pip-tools; \
		pip-compile --resolver=backtracking --output-file ${REPO_ROOT}/pip.lock ${REPO_ROOT}/setup.cfg; \
	fi
	@if [ "${SKIP_LOCKFILE_INSTALL}" = "n" ]; then \
		pip install -r ${REPO_ROOT}/pip.lock; \
		pip install --no-deps -e ${REPO_ROOT}; \
	fi

full: # Run a "full" install, QC, test, and build. You'll need to have the environment already activated even though it rebuilds it.
	$(MAKE) build-env install INSTALL_EXTRAS=[dev] full-qc full-test build-doc build-package

full-qc: # Run all the QC.
	$(MAKE) lint security typecheck

full-test: # Run all the tests.
	$(MAKE) unit integration e2e

clean: # Clear caches and coverage reports, etc.
	@cd ${REPO_ROOT} && rm -rf .pytest_cache .pytype .coverage* cov_report* *_test_report.xml
	$(shell find ${REPO_ROOT} -type f -name '*py[co]' -delete -o -type d -name __pycache__ -delete)

format: # Clean up code.
	black --config ${REPO_ROOT}/pyproject.toml ${QC_DIRS}
	isort -p ${PACKAGE_NAME} --settings-path ${REPO_ROOT}/pyproject.toml ${QC_DIRS}

lint: # Check style and formatting. Should agree with format and only catch what format can't fix, like line length, missing docstrings, etc.
	black --config ${REPO_ROOT}/pyproject.toml --check ${QC_DIRS}
	isort -p ${PACKAGE_NAME} --settings-path ${REPO_ROOT}/pyproject.toml --check-only ${QC_DIRS}
	flake8 --config ${REPO_ROOT}/.flake8 ${QC_DIRS}

security: # Check for vulnerabilities.
	bandit -r ${REPO_ROOT}/src
ifeq ( "$(VULNERABILITIES_ENGINE)", "safety" )
	ifeq( "${IGNORE_SAFETY}","y" )
		-safety check
	else
		safety check
	endif
endif
ifeq ( "${VULNERABILITIES_ENGINE}", "jake" )
	ifeq ( "${IGNORE_JAKE}", "y")
		-pip list | jake ddt
		-conda list --json | jake ddt --type=CONDA_JSON
	else
		pip list | jake ddt
		conda list --json | jake ddt --type=CONDA_JSON
	endif
endif

typecheck: # Check typing.
	pytype --config=${REPO_ROOT}/pytype.cfg -- ${QC_DIRS}

# Parametrized test function for the test targets (unit, integration, e2e).
test_function = export COVERAGE_FILE=${REPO_ROOT}/.coverage.$(1)_${PYTHON_VERSION} && \
	pytest -m $(1) ${REPO_ROOT} \
	--rootdir ${REPO_ROOT} \
	--durations=0 \
	--durations-min=1.0 \
	-c ${REPO_ROOT}/pyproject.toml \
	--cov \
	--cov-report term \
	--cov-report html:${REPO_ROOT}/cov_report_$(1)_${PYTHON_VERSION} \
	# --cov-config=${REPO_ROOT}/.coveragerc

unit: # Run unit tests.
	$(call test_function,unit)

integration: #Run integration tests.
	$(call test_function,integration)

e2e: # Run end-to-end tests.
	$(call test_function,e2e)

build-doc: # Build Sphinx docs, from autogenerated API docs and human-written RST files.
	@if [ -d "${REPO_ROOT}/docs/_build" ]; then rm -r ${REPO_ROOT}/docs/_build; fi
	mkdir ${REPO_ROOT}/docs/_build
	sphinx-apidoc -o ${REPO_ROOT}/docs ${REPO_ROOT}/src/${PACKAGE_NAME} -f
	sphinx-build ${REPO_ROOT}/docs ${REPO_ROOT}/docs/_build

build-package: # Build the package to deploy.
	python -m build ${REPO_ROOT}
	twine check ${DIST_DIR}/*

ci-run: # Run the CI-CD workflow.
	$(eval MATRIX_OS_FLAG := $(if $(MATRIX_OS),--matrix os:${MATRIX_OS},))
	$(eval MATRIX_PYTHON_VERSION_FLAG := $(if $(MATRIX_PYTHON_VERSION),--matrix python-version:${MATRIX_PYTHON_VERSION},))
	act ${ACT_RUN_EVENT} ${MATRIX_OS_FLAG} ${MATRIX_PYTHON_VERSION_FLAG}
