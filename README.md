# Bellingham Food Bank delivery planning toolkit

## Summary

This doesn't do much yet. It is made from the `reference_package` template repo: https://github.com/crickets-and-comb/reference_package

The plan is to use this package for some of the tasks food bank staff do manually to plan the delivery routes.

They currently use Circuit (https://getcircuit.com), but there are some tedious tasks to prepare the data for Circuit and then to process the data after using Circuit. They currently upload all the stops they need to Circuit to produce a single huge route, then they manually chunk up the route by driver according to how many boxes a driver can carry and what is a sensible set of stops, and finally they upload those smaller routes to Circuit again to optimize them. They spend several hours each week on the manual pieces of this, the chunking alone taking about four hours.

So far, the package just splits a spreadsheet of delivery stops labeled by driver into n workbooks (1 per staff member working on the route generation), one workbook sheet per driver. This allows staff to split the task of submitting unique driver routes to Circuit. The tool for this is called `split_chunked_route`. See below for usage.

## Dev plan

Without replacing Circuit, there are some processes that can be further automated:
- Chunking by driver: This may be the most challenging piece, I'm not confident I can solve this well enough to justify using my solution. So, I will save it for after I've cleared some of the low-hanging fruit. My first plan of attack is to try using k-nearest neighbors.
- Splitting the chunked-by-driver list into a single workbook with separate sheets/CSVs by driver for upload and rerouting. Easy-peasy.
- Combining the output by driver into a single workbook. Easy-peasy.
- Formatting those sheets into the final sheets used for records and printing for drivers. They currently have a spreadsheet macro do most of this, but there are some pieces they still need to do manually. These solutions could probably be implemented in the spreadsheet, and that may be the best solution if they want to keep the macro. But, it might be simpler to replace the macro with formatting at the end of the ETL above.
- Uploading and exporting can be done via the Circuit API, which would enable the above steps to be wrapped into a single ETL pipeline.

The plan of attack is to start with the low-hanging fruit of data formatting before moving onto the bigger problem of chunking. Integrating with the Circuit API may come before or after the chunking solution, depending on how complicated each proves.

## Structure

```bash
    src/bfb_delivery/api            Public and internal API.
    src/bfb_delivery/cli            Command-line-interface.
    src/bfb_delivery/lib            Implementation.
    tests/e2e                       End-to-end tests.
    test/integration                Integration tests.
    tests/unit                      Unit tests.
```

## Dependencies

* Python 3.11
* [make](https://www.gnu.org/software/make/)


## Public API

`bfb_delivery` is a library from which you can import functions. Import the public `split_chunked_route` function like this:

```python
    from bfb_delivery import split_chunked_route
    # These are okay too:
    # from bfb_delivery.api import split_chunked_route
    # from bfb_delivery.api.public import split_chunked_route
```

Or, if you're a power user and want any extra options that may exist, you may want to import the internal version like this:

```python
    from bfb_delivery.api.internal import split_chunked_route
```

Unless you're developing, avoid importing directly from library:

```python
    # Don't do this:
    from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route
```

## CLI

Try the CLI with this package installed:

    $ split_chunked_route --input_path "some/path_to/raw_chunked_sheet.xlsx"

See other options in the help menu:

    $ split_chunked_route --help



## Dev installation

You'll want this package's site-package files to be the source files in this repo so you can test your changes without having to reinstall. We've got some tools for that.

First build and activate the env before installing this package:

    $ make build-env
    $ conda activate bfb_delivery_py3.12

(Note, you will need Python activated, e.g. via conda base env, for `build-env` to work, since it uses Python to grab `PACKAGE_NAME` in the Makefile. You could alternatively just hardcode the name.)

Then, install this package and its dev dependencies:

    $ make install INSTALL_EXTRAS=[dev]

This installs all the dependencies in your conda env site-packages, but the files for this package's installation are now your source files in this repo.

## Dev workflow

You can list all the make tools you might want to use:

    $ make list-targets

Go check them out in `Makefile`.

### QC and testing

Before pushing commits, you'll usually want to rebuild the env and run all the QC and testing:

    $ make clean full

When making smaller commits, you might just want to run some of the smaller commands:

    $ make clean format full-qc full-test

### CI test run

Before opening a PR or pushing to it, you'll want to run locally the same CI pipeline that GitHub will run (`.github/workflows/QC-and-build.yml`). This runs on multiple images, so you'll need to install Docker and have it running on your machine: https://www.docker.com/

Once that's installed and running, you can use `act`. You'll need to install that as well. I develop on a Mac, so I used `homebrew` to install it (which you'll also need to install: https://brew.sh/):

    $ brew install act

Then, run it from the repo directory:

    $ make ci-run

That will run `.github/workflows/QC-and-build.yml` and every other action tagged to the pull_request event. Also, since `act` doesn't work with Mac and Windows architecture, it skips/fails them, but it is a good test of the Linux build.