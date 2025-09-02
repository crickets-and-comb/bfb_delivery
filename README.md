# Bellingham Food Bank delivery planning toolkit

This set of command-line tools cuts some cruft around creating delivery route manifests for the Bellingham Food Bank. It saves an estimated five paid staff hours per week, along with removing much of the room for error. See the docs for user guides: https://cricketsandcomb.org/bfb_delivery/.

This is a [Crickets and Comb](https://cricketsandcomb.org) solution.

## What it solves

The food bank uses Circuit (https://getcircuit.com) to create optimized routes from lists of addresses and products, but there were some tedious tasks to prepare the data for Circuit and then to format the optimized routes into manifests for printing. It took several hours each week. Staff don't need to do that anymore now that they use the tool deployed with this package. Previously, they would:

0. Put all the stops in a single spreadsheet.
1. Upload stops to Circuit to produce a single huge route as a starting point.
2. Download the optimized route.
3. Manually "chunk" the route by driver (assign stops to drivers according to how many boxes a driver can carry, what is a sensible set of stops, per-driver constraints, etc.).
4. Split those routes into separate worksheets.
5. Upload those smaller routes to Circuit again.
6. Set attributes etc., launch optimization, and distribute to drivers.
7. Download the optimized CSVs.
8. Combine the output CSVs into a single Excel workbook with a worksheet for each route.
9. Finally format the sheets into printable manifests with a combination of Excel macro and manual steps.

Staff would spend several hours each week on the manual pieces of this, the chunking (step 3) alone taking about four hours. Now staff need only do the chunking, and step 0 of collecting all the stops, because the `bfb_delivery` package will do the rest.

## Dev plan

We have no intention or desire to replace Circuit. In addition to optimizing routes, Circuit pushes routes to an app drivers can use, etc. But, there are some processes outside of that that could be further automated or supported with tools:

- Chunking by driver (step 3 above): This may be the most challenging piece. I'm only a little confident I can solve this well enough to justify using my solution. So, I have saved it for after I've cleared the low-hanging fruit. My first inclination is to try using a sort of recursive k-nearest neighbors to group stops into potential routes, but that may change once I research existing routing algorithms.

- To that end, implementing a mapping tool to check routes will be helpful both in development and production.

- There are additional constraints to consider per driver. It may not be possible to encode all of them, but knocking out some of them may help cut down time, and working on this before taking on the chunking problem will better define the problem and add some validations to assist staff.

- DB: There's no plan to develop, host, and support a DB. We're using Excel, CSVs, etc. to keep close to users' knowledge and skill bases, and to keep close to the old manual workflow and resources. A DB would be especially useful for encoding driver restrictions etc., but a simple spreadsheet or JSON doc should suffice. If we did start using a DB, however, we'd need to create CRUD interfaces to it.

- GUI: This would be a desktop installation so users can click and select input files, enter other params, assign routes to drivers, and click to open the final output file. A couple of UX/UI developers may be taking that on at time of writing.

The plan of attack has been to start with the low-hanging fruit of ETL before moving onto the bigger problem of chunking. Fully integrating with the Circuit API is the last step before taking on the chunking, and that is complete. We've put it into production and are seeing what arises (bugs, feature requests, etc.) before moving on. (Also, my attention needs to shift to finding paying work before I launch into anything serious again for this project.)

### Frankenstein's "Agile" caveat

The main tool wraps nested tools. This is a natural developmental result of incrementally and tentatively taking over this workflow as a volunteer as I gained trust and access to the org's data, information, and resources. Also, the project was largely unsolicited (but fully approved), so I was hesitant to ask too much of the staff to define and clarify requirements etc.

A benefit of having these subtools wrapped within the larger tool is that it produces intermediate outputs and maintains backwards compatability that can be rolled back to the old methods for a given step should it fail for some reason, without the need to do the whole process over again.

There are certainly improvements that can be made, so please take a look at the issues in the GitHub repo.

## Structure

```
    .github/workflows               GitHub Actions CI/CD workflows.
    docs                            RST docs and doc build staging.
    Makefile                        Dev tools and params. (includes shared/Makefile)
    scripts                         Scripts for running tests etc. with real data if you have it.
    setup.cfg                       Metadata and dependencies.
    shared                          Shared dev tools Git submodule.
    src/reference_package/api       Public and internal API.
    src/reference_package/cli       Command-line-interface.
    src/reference_package/lib       Implementation.
    tests/e2e                       End-to-end tests.
    test/integration                Integration tests.
    tests/unit                      Unit tests.
```

## Dependencies

* Python 3.11
* [make](https://www.gnu.org/software/make/)

See `setup.cfg` for installation requirements.

## Installation

Run `pip install bfb_delivery`. See https://pypi.org/project/bfb-delivery/.

## Usage Examples

See docs for full usage: https://crickets-and-comb.github.io/bfb_delivery/

### Public API

`bfb_delivery` is a library from which you can import functions. Import the public `build_routes_from_chunked` function like this:

```python
    from bfb_delivery import build_routes_from_chunked
    # These are okay too:
    # from bfb_delivery.api import build_routes_from_chunked
    # from bfb_delivery.api.public import build_routes_from_chunked
```

Or, if you're a power user and want any extra options that may exist, you may want to import the internal version like this:

```python
    from bfb_delivery.api.internal import build_routes_from_chunked
```

Unless you're developing, avoid importing directly from library:

```python
    # Don't do this:
    from bfb_delivery.lib.dispatch.write_to_circuit import build_routes_from_chunked
```

### CLI

Try the CLI with this package installed:

```bash
    $ build_routes_from_chunked --input_path "some/path_to/raw_chunked_sheet.xlsx"
```

See other options in the help menu:

```bash
    $ build_routes_from_chunked --help
```

CLI tools (see docs for more information):

- build_routes_from_chunked
- split_chunked_route
- create_manifests_from_circuit
- create_manifests
- combine_route_tables
- format_combined_routes


## Developers

### Setting up shared tools

There are some shared dev tools in a Git submodule called `shared`. See https://github.com/crickets-and-comb/shared. When you first clone this repo, you need to initialize the submodule:

```bash
    $ git submodule init
    $ git submodule update
```

See https://git-scm.com/book/en/v2/Git-Tools-Submodules

### Dev installation

You'll want this package's site-package files to be the source files in this repo so you can test your changes without having to reinstall. We've got some tools for that.

First build and activate the env before installing this package:

```bash
    $ make build-env
    $ conda activate bfb_delivery_py3.12
```

(Note, you will need Python activated, e.g. via conda base env, for `build-env` to work, since it uses Python to grab `PACKAGE_NAME` in the Makefile. You could alternatively just hardcode the name.)

Then, install this package and its dev dependencies:

```bash
    $ make install
```

This installs all the dependencies in your conda env site-packages, but the files for this package's installation are now your source files in this repo.

### Dev setup

Sign up for Safety CLI at https://platform.safetycli.com and get an API-key. Additionally you'll have to create a personal access token on GitHub. 

Add both the API-key and your personal access token to your .env:

```bash
SAFETY_API_KEY=<your_key>
CHECKOUT_SHARED=<your_access_token>
```

Finally, if running from a forked repo, add your safety-API-key as a secret on GitHub. You can do this under security/secrets and variables/actions in your repo settings.

```bash
SAFETY_API_KEY=<your_key>
```

### Dev workflow

You can list all the make tools you might want to use:

```bash
    $ make list-makes
```

Go check them out in `Makefile`.

### Live-test helper scripts

There are some useful scripts for live-testing in 'scripts/':
| script | functionality | bash |
| ------ | ------------- | ------------- |
| `delete_plan.py` | Deletes/cancels every route listed in a `plan.csv` **or** a single plan by ID. Note that 'plan.csv' is typically written to the plans/ subdirectory of the output folder. | `python scripts/delete_plan.py --plan_df_fp path/to/plan.csv`<br>`python scripts/delete_plan.py --plan_id plans/{id}` |
| `retrieve_plan.py` | Pulls the latest state of a plan from Circuit and returns a JSON. | `python scripts/retrieve_plan.py --plan-id plans/123456` |
| `mock_run_e2e.py` | Allows you to mock the workflow end to end by generating mock CSVs in place of Circuit's API responses. | `python scripts/mock_run_e2e.py` |

#### QC and testing

Before pushing commits, you'll usually want to rebuild the env and run all the QC and testing:

```bash
    $ make clean full
```

When making smaller commits, you might just want to run some of the smaller commands:

```bash
    $ make clean format full-qc full-test
```

#### CI test run

Before opening a PR or pushing to it, you'll want to run locally the same CI pipeline that GitHub will run (`.github/workflows/CI_CD.yml`). This runs on multiple images, so you'll need to install Docker and have it running on your machine: https://www.docker.com/

Once that's installed and running, you can use `act`. You'll need to install that as well. I develop on a Mac, so I used `homebrew` to install it (which you'll also need to install: https://brew.sh/):

```bash
    $ brew install act
```

Then, run it from the repo directory:

```bash
    $ make run-act
```

That will run `.github/workflows/CI_CD.yml`. Also, since `act` doesn't work with Mac and Windows architecture, it skips/fails them, but it is a good test of the Linux build.

NOTE: To be more accurate, we've overridden `run-act` to create a local `CI_CD_act.yml` (which we ignore with Git) as a copy of `CI_CD.yml` and replace one of the workflow call URLs with a relative path. We use a relative path because otherwise `act` will not honor the overridden `full-test` make target and will run the shared version. That will fail because the shared `full-test` target includes running integration and e2e tests, which this repo does not include.

### See also

See also [https://cricketsandcomb.org/bfb_delivery/developers.html](https://cricketsandcomb.org/bfb_delivery/developers.html).

## Acknowledgments

This package is made from the `reference_package` template repo: https://github.com/crickets-and-comb/reference_package.
