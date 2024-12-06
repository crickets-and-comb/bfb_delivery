# Basic package template

## Summary

Just a basic package template.

## Credit

I borrowed and modified the structure and tools from the idiomatic usage of IHME's Central Computation GBD team when I worked with them in 2023-2024.

## Structure

```bash
    src/bfb_delivery/api       Public and internal API.
    src/bfb_delivery/cli       Command-line-interface.
    src/bfb_delivery/lib       Implementation.
    tests/e2e                       End-to-end tests.
    test/integration                Integration tests.
    tests/unit                      Unit tests.
```

## Dependencies

* Python 3.11
* [make](https://www.gnu.org/software/make/)


## Library functions

`bfb_delivery` is a library from which you can import functions. Import the public example function like this: `from bfb_delivery import wait_a_second`. Or, import the internal version like a power user like this: `from bfb_delivery.api.internal import wait_a_second`.

Unless you're developing, avoid importing directly from library, like `from bfb_delivery.lib.example import wait_a_second`.

## CLI

Try the example CLI:

    $ python -m example
    $ python -m example --secs 2

## Dev installation

You'll want this package's site-package files to be the source files in this repo so you can test your changes without having to reinstall. We've got some tools for that.

First build and activate the env before installing this package:

    $ make build-env
    $ conda activate reference_package_py3.12

Then, install this package and its dev dependencies:

    $ make install INSTALL_EXTRAS=[dev]

This installs all the dependencies in your conda env site-packages, but the files for this package's installation are now your source files in this repo.

## Dev workflow

You can list all the make tools you might want to use:

    $ make list-targets

Go check them out in `Makefile`.

### QC and testing

Before pushing commits, you'll usually want to rebuild the env and run all the QC and testing:

    $ make clean format full

When making smaller commits, you might just want to run some of the smaller commands:

    $ make clean format full-qc full-test

### CI test run

Before opening a PR or pushing to it, you'll want to run locally the same CI pipeline that GitHub will run (`.github/workflows/QC-and-build.yml`). This runs on multiple images, so you'll need to install Docker and have it running on your machine: https://www.docker.com/

Once that's installed and running, you can use `act`. You'll need to install that as well. I develop on a Mac, so I used `homebrew` to install it (which you'll also need to install: https://brew.sh/):

    $ brew install act

Then, run it from the repo directory:

    $ make ci-run

That will run `.github/workflows/QC-and-build.yml` and every other action tagged to the pull_request event. Also, since `act` doesn't work with Mac and Windows architecture, it skips/fails them, but it is a good test of the Linux build.