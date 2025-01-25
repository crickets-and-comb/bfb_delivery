# Bellingham Food Bank delivery planning toolkit

## Summary

This set of command-line tools cuts some cruft around creating delivery route manifests. It's made from the `reference_package` template repo: https://github.com/crickets-and-comb/reference_package. See the docs: https://crickets-and-comb.github.io/bfb_delivery/.

The plan is to continue to build this package out to take on more of the tasks food-bank staff do manually to plan the delivery routes.

They use Circuit (https://getcircuit.com) to create optimized routes from lists of addresses and products, but there are some tedious tasks to prepare the data for Circuit and then to format the optimized routes into manifests for printing:

0. Put all the stops in a single spreadsheet.
1. Upload stop to Circuit to produce a single huge route as a starting point.
2. Download the optimized route.
3. Manually "chunk" the route by driver (assign stops to drivers according to how many boxes a driver can carry, what is a sensible set of stops, per-driver constraints, etc.).
4. Split those routes into separate worksheets.
5. Upload those smaller routes to Circuit again to optimize them.
6. Set attributes etc. and launch optimization.
7. Download the optimized CSVs.
8. Combine the output CSVs into a single Excel workbook with a worksheet for each route.
9. Finally format the sheets into printable manifests with a combination of Excel macro and manual steps.

Staff spend several hours each week on the manual pieces of this, the chunking alone taking about four hours.

## What it does so far

At this point, the `bfb_delivery` package will:

2. Download the optimized route.
4. Split those routes into separate worksheets.
7. Download the optimized CSVs.
8. Combine the output CSVs into a single Excel workbook with a worksheet for each route.
9. Finally format the sheets into printable manifests with a combination of Excel macro and manual steps.

The last three steps exectue in a single command. Next up is wrapping that up into a single command that also uploads the routes and optimizes them on Circuit, dispatches them, and then creates the final manifests -- leaving only the "chunking" step (step 3) for the hoomans.

See docs for more details: https://crickets-and-comb.github.io/bfb_delivery/.

## Dev plan

We have no intention or desire to replace Circuit. In addition to optimizing routes, Circuit pushes routes to an app drivers can use, etc. But, there are some processes outside of that to further automate/support:

- Chunking by driver (step 3 above): This may be the most challenging piece. I'm only a little confident I can solve this well enough to justify using my solution. So, I will save it for after I've cleared the low-hanging fruit. My first inclination is to try using a sort of recursive k-nearest neighbors (KNN) to group stops into potential routes, but that may change once I research existing routing algorithms.

- To that end, implementing a mapping tool to check routes will be helpful in both dev and production. This would be essential to removing the first upload to Circuit to make a long dummy route, if we're able to build a sufficient a chunking solution. (That first upload doesn't cost the food bank anything but a trivial amount of time and fiddling around, but matching its sufficiency as a starting point is a good first benchmark.)

- There are additional constraints to consider per driver. It may not be possible to encode all of them, but knocking out some of them may help cut down time, and doing this before chunking will better define the problem and add some validations.

- The Circuit API supports uploading, optimizing, and exporting, which enables wrapping more of the steps into a single ETL pipeline. We have built the required export segment of the pipeline, and now need to build the upload and optimizing segments. There may be limitations to what attribute mapping, entity updates, etc. can be done via API.

- DB: There's no plan to develop, host, and support a DB. We're using Excel, CSVs, etc. to keep close to users' knowledge and skill bases, and existing workflow and resources. A DB would be especially useful for encoding driver restrictions etc., but a simple spreadsheet should suffice. If we did start using a DB, however, we'd need to create CRUD interfaces to it.

- GUI: There's no real plan to develop a GUI, but it might be a good portfolio project to try out Vercel or something.

The plan of attack has been to start with the low-hanging fruit of ETL before moving onto the bigger problem of chunking. Fully integrating with the Circuit API is the last step before taking on the chunking, and the integration is now halfway done with read capability.

### Comment on the piecemeal nature

The tools thus far are a series of small tools that were then wrapped by bigger tools, which were then wrapped by a larger tool, etc. This is a natural developmental result of incrementally and tentatively taking over this workflow as a volunteer as I gained trust and access to the org's data, information, and resources. Also, the project was largely unsolicited (but fully approved), so I was hesitant to ask too much of the staff to define and clarify requirements etc. It hasn't been used yet by staff in production (though certainly tested live), so I haven't yet discovered any of the improvements or mistaken assumptions that real users will find. As such, there are some optmizations that could happen to reduce the unnecessary output etc., but I am hesitant to do that until we find what works best for the users.

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

## Installation

Run `pip install bfb_delivery`. See https://pypi.org/project/bfb-delivery/.

## Usage Examples

See docs for full usage.

### Public API

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

### CLI

Try the CLI with this package installed:

    $ split_chunked_route --input_path "some/path_to/raw_chunked_sheet.xlsx"

See other options in the help menu:

    $ split_chunked_route --help

CLI tools (see docs for more information):

- split_chunked_route
- create_manifests
- combine_route_tables
- format_combined_routes


## Dev

### Setting up shared tools

There are some shared dev tools in a Git submodule called `shared`. See https://github.com/crickets-and-comb/shared. When you first clone this repo, you need to initialize the submodule:

    $ git submodule init
    $ git submodule update

See https://git-scm.com/book/en/v2/Git-Tools-Submodules

### Dev installation

You'll want this package's site-package files to be the source files in this repo so you can test your changes without having to reinstall. We've got some tools for that.

First build and activate the env before installing this package:

    $ make build-env
    $ conda activate bfb_delivery_py3.12

(Note, you will need Python activated, e.g. via conda base env, for `build-env` to work, since it uses Python to grab `PACKAGE_NAME` in the Makefile. You could alternatively just hardcode the name.)

Then, install this package and its dev dependencies:

    $ make install

This installs all the dependencies in your conda env site-packages, but the files for this package's installation are now your source files in this repo.

### Dev workflow

You can list all the make tools you might want to use:

    $ make list-targets

Go check them out in `Makefile`.

#### QC and testing

Before pushing commits, you'll usually want to rebuild the env and run all the QC and testing:

    $ make clean full

When making smaller commits, you might just want to run some of the smaller commands:

    $ make clean format full-qc full-test

#### CI test run

Before opening a PR or pushing to it, you'll want to run locally the same CI pipeline that GitHub will run (`.github/workflows/QC-and-build.yml`). This runs on multiple images, so you'll need to install Docker and have it running on your machine: https://www.docker.com/

Once that's installed and running, you can use `act`. You'll need to install that as well. I develop on a Mac, so I used `homebrew` to install it (which you'll also need to install: https://brew.sh/):

    $ brew install act

Then, run it from the repo directory:

    $ make run-act

That will run `.github/workflows/QC-and-build.yml` and every other action tagged to the pull_request event. Also, since `act` doesn't work with Mac and Windows architecture, it skips/fails them, but it is a good test of the Linux build.