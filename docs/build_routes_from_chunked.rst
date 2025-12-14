========================================================================
Build, Optimize, and Distribute Circuit Routes from Chunked Routes Sheet
========================================================================

Once you have a large single-route sheet labeled by driver, representing individual driver routes, you need to upload them to Circuit, optimized them, distribute them to driver apps, and produce driver manifests ready to print, with headers, aggregate data, and color-coded box types. You can do this all at once with the :code:`build_routes_from_chunked` tool.

This tool replaces the manual tasks of:

- Splitting the single route grouped by driver into individual Excel worksheets for each driver, grouped into a handful of workbooks for each staff member to upload to Circuit.
- Uploading to Circuit, mapping fields and routes to drivers, optimizing routes, and distributing them to driver apps.
- Downloading the optimized routes, copying each route into a single workbook, running an Excel macro, and finishing up with some manual steps.

.. important::

    Before using this tool, you need to log in to Circuit and activate the drivers you want to assign routes to.

Python API documentation at :py:func:`bfb_delivery.api.public.build_routes_from_chunked`.

CLI documentation at :doc:`CLI`.

Usage
-----

You call :code:`build_routes_from_chunked` with the :code:`input_path` of the chunked spreadsheet, along with any other optional arguments. It will then prompt you to confirm driver assignments to each route. The tool then runs for up to a minute per route and finally prints the filepath to the final manifest.


In Python:

.. code:: python

    from bfb_delivery import build_routes_from_chunked

    build_routes_from_chunked(input_path="path/to/master_chunked_sheet.xlsx")

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx"

The function will return the filepath to the final manifest. If you're using the CLI, the filepath will print to the console.

.. note::

    This takes up to a minute per route to run as it uploads the routes to Circuit, optimizes them, and downloads the optimized routes. So, for a typical number of routes, it will take about 30 minutes.

Examining output
~~~~~~~~~~~~~~~~

Below is an example with three routes (usually about 40-50 routes). The tool will prompt you to assign drivers to each route, e.g. "Enter the number of the driver for '02.14 Eric'(ctl+c to start over):". You use the driver numbers printed to the console to assign drivers to each route, 48 in this example (see input below). The tool will then run for up to a minute per route and finish successfully by printing the filepath to the final manifest.

.. note::

    Notice how the driver status is printed with the driver. The tool will not allow you to assign an inactive driver to a route, re-prompting you if you do enter an inactive driver. If you need to assign an inactive driver, you will need to activate them in Circuit first.

.. code-block::

    (bfb_delivery_py3.12) MacBook-Pro:bfb_delivery me$ build_routes_from_chunked --input_path .test_data/scratch/test_master_chunked_sample.xlsx --output_dir .test_data/scratch/
    2025-02-12 17:49:38,585 - INFO - Writing split chunked workbooks to /Users/me/repos/bfb_delivery/.test_data/scratch/split_chunked
    2025-02-12 17:49:38,620 - INFO - Getting all drivers from Circuit ...
    2025-02-12 17:49:39,568 - INFO - Finished getting drivers.
    1. Active: Hank Hill hank@propane.com
    ... (more drivers) ...
    84. Active: Nosferatu pinebox@bfb.com

    Using the driver numbers above, assign drivers to each route:

    Route 02.14 Eric:
    Best guesses:
    48. Active: Eric Cartman emoney@southpark.com


    Enter the number of the driver for '02.14 Eric'(ctl+c to start over):48

    Assigned 02.14 Eric to Eric Cartman.

    Route 02.14 Hank:
    Best guesses:
    1. Active: Hank Hill hank@propane.com
    23. Inactive: Hankie Who hankie@realtalk.com


    Enter the number of the driver for '02.14 Hank'(ctl+c to start over):1

    Assigned 02.14 Hank to Hank Hill.

    Route 02.14 Nos:
    Best guesses:
    84. Active: Nosferatu pinebox@bfb.com


    Enter the number of the driver for '02.14 Nos'(ctl+c to start over):84

    Assigned 02.14 Nos to Nosferatu.
    02.14 Eric: Eric Cartman, emoney@southpark.com
    02.14 Hank: Hank Hill, hank@propane.com
    02.14 Nos: Nosferatu pinebox@bfb.com
    Confirm the drivers above? (y/n): y
    2025-02-12 17:50:16,179 - INFO - Initializing plans ...
    2025-02-12 17:50:18,067 - INFO - Finished initializing plans. Initialized 3 plans.
    2025-02-12 17:50:18,126 - INFO - Uploading stops. Allow 6+ seconds per plan ...
    2025-02-12 17:50:44,995 - INFO - Finished uploading stops. Uploaded 50 stops for 3 plans.
    2025-02-12 17:50:45,023 - INFO - Initializing route optimizations. Allow 20+ seconds per plan ...
    2025-02-12 17:51:49,759 - INFO - Finished initializing route optimizations for 3 plans.
    2025-02-12 17:51:49,782 - INFO - Confirming optimizations have finished ...
    2025-02-12 17:51:50,980 - INFO - Finished optimizing routes. Optimized 3 routes.
    2025-02-12 17:51:51,019 - INFO - Distributing routes ...
    2025-02-12 17:51:53,200 - INFO - Finished distributing routes for 3 plans.
    2025-02-12 17:51:53,222 - INFO - 
        route_title  initialized  writable  stops_uploaded optimized  distributed
    0  02.14 Eric             True      True            True      True         True
    1  02.14 Hank             True      True            True      True         True
    2  02.14 Nos              True      True            True      True         True

    Plans attempted: 3
    Plans initialized: 3
    Plans with stops: 3
    Plans optimized: 3
    Plans distributed: 3
    2025-02-12 17:51:53,231 - INFO - Getting route plans from Circuit ...
    2025-02-12 17:51:53,692 - INFO - Finished getting route plans.
    2025-02-12 17:51:53,711 - INFO - Getting stops from Circuit ...
    2025-02-12 17:51:57,597 - INFO - Finished getting stops.
    2025-02-12 17:51:57,629 - WARNING - Missing neighborhood for 50 stops. Imputing best guesses from Circuit-supplied address.
    2025-02-12 17:51:57,655 - WARNING - Output directory exists /Users/me/repos/bfb_delivery/.test_data/scratch/routes_2025-02-14. Overwriting.
    2025-02-12 17:51:57,655 - INFO - Writing route CSVs to /Users/me/repos/bfb_delivery/.test_data/scratch/routes_2025-02-14
    2025-02-12 17:51:57,676 - INFO - Writing combined routes to /Users/me/repos/bfb_delivery/.test_data/scratch/combined_routes_20250212.xlsx
    2025-02-12 17:51:57,711 - INFO - Writing formatted routes to /Users/me/repos/bfb_delivery/.test_data/scratch/final_manifests_20250212.xlsx
    2025-02-12 17:51:57,715 - INFO - Formatted workbook saved to:
    /Users/me/repos/bfb_delivery/.test_data/scratch/final_manifests_20250212.xlsx
    (bfb_delivery_py3.12) MacBook-Pro:bfb_delivery me$

.. attention::

    Pay attention to the output, especially in the middle once optimization/distribution is complete. There's a summary of the number of plans attempted, initialized, with stops, optimized, and distributed. Just above that is a table of the routes and their statuses, :code:`True` indicating success for a route at a given step, :code:`False` indicating failure. You can also find the table as a CSV file in a subdirectory of the output directory, :code:`{output_dir}/plans/plans.csv`. If any of the routes are not successul at a step, the tool will warn you and skip them on all following steps. You will want to investigate why and run that route the rest of the way from the last successful step, manually or with the underlying tools (see :ref:`tools_wrapped_note` below).

Note that the filepaths to some intermediate files will print to the console as well, before finally printing the filepath to the final manifest workbook. In addition to the final manifest, there will be:

- An Excel workbook of the pre-optmized routes split into separate spreadsheets that you could upload manually. In the example: ``2025-02-12 17:49:38,585 - INFO - Writing split chunked workbooks to /Users/me/repos/bfb_delivery/.test_data/scratch/split_chunked``
- Optmized route CSVs downloaded from Circuit that you could combine into a single Excel workbook. In the example: ``2025-02-12 17:51:57,655 - INFO - Writing route CSVs to /Users/me/repos/bfb_delivery/.test_data/scratch/routes_2025-02-14``
- An unformatted Excel workbook of all the routes that you could run the old macro on to produce the final manifests. In the example: ``2025-02-12 17:51:57,676 - INFO - Writing combined routes to /Users/me/repos/bfb_delivery/.test_data/scratch/combined_routes_20250212.xlsx``

This can be helpful if you need to revert to the old method for one of the steps for some reason.

Reviewing final manifests
~~~~~~~~~~~~~~~~~~~~~~~~~

You should review the manifests before printing them, as you may want to make some final touchups, like adjusting row heights or column widths, or adding notes. These final touchups are slated to be added to the tool in the future.

Optional arguments
~~~~~~~~~~~~~~~~~~

You can use optional arguments to specify a few things about the manifest workbook. Use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    build_routes_from_chunked --help

Output directory
^^^^^^^^^^^^^^^^

Use the optional argument :code:`output_dir` to specify the directory to save the workbook file in. The default if not passed is a new directory in the present working directory, named "deliveries_{date}".

In Python:

.. code:: python

    build_routes_from_chunked(
        input_path="path/to/master_chunked_sheet.xlsx",
        output_dir="path/to/output_dir/",
    )

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx" --output_dir "path/to/output_dir/"

Start date
^^^^^^^^^^

Use the optional argument :code:`start_date` to specify the beginning of the date range to search Circuit for routes. The default if not passed in is the soonest Friday.

.. code:: python

    from bfb_delivery import build_routes_from_chunked

    build_routes_from_chunked(
        input_path="path/to/master_chunked_sheet.xlsx",
        start_date="1947-10-14",
    )

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx" --start_date 1957-10-04

Skip distribution
^^^^^^^^^^^^^^^^^

By default, the tool will distribute the routes to the driver apps. Use the optional argument :code:`no_distribute` to skip this.

In Python:

.. code:: python

    build_routes_from_chunked(
        input_path="path/to/master_chunked_sheet.xlsx",
        no_distribute=True,
    )

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx" --no_distribute

Verbose output
^^^^^^^^^^^^^^

Use the optional argument :code:`verbose` to print more information to the console. This can be useful for debugging, but it is pretty noisy.

In Python:

.. code:: python

    build_routes_from_chunked(
        input_path="path/to/master_chunked_sheet.xlsx",
        verbose=True,
    )

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx" --verbose

Supplying extra notes
^^^^^^^^^^^^^^^^^^^^^

Use the optional argument :code:`extra_notes_file` to specify a CSV file with extra notes to include in the manifest. The CSV file should have two columns: :code:`tag` and :code:`note`. The tag is the text (usually asterisked) that appears in the standard notes field for a delivery. The note is then added to the bottom of the manifest with the tag. For example:

.. code-block:: text

    tag,note
    Cedarwood Apartments special instructions *,Please call the recipient when you arrive.

This file will put the note "Please call the recipient when you arrive." at the bottom of the manifest (once) if any stops have a note that contains the text "Cedarwood Apartments special instructions \*".

If you don't provide :code:`extra_notes_file` provide, the tool will use the constant notes in the codebase: :py:data:`bfb_delivery.lib.constants.ExtraNotes` (currently empty).

In Python:

.. code:: python

    build_routes_from_chunked(
        input_path="path/to/master_chunked_sheet.xlsx",
        extra_notes_file="path/to/extra_notes.csv",
    )

With CLI:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked_sheet.xlsx" --extra_notes_file path/to/extra_notes.csv

.. note::

    Extra notes are placed in merged cells with automatic row height calculation. The height calculation is approximate and may not be perfect for all text lengths and formatting. Manual review of the cell heights is recommended to ensure notes are fully visible.

.. _tools_wrapped_note:

Note on tools this tool wraps
-----------------------------

:code:`build_routes_from_chunked` wraps other tools that run each segment of the pipeline. First it runs :code:`split_chunked_route` to split the master chunked sheet into spreadsheets for each route in a single workbook before uploading. It then runs :code:`create_manifests_from_circuit`, so you don't have to download and move files around and format them. :code:`create_manifests_from_circuit` actually wraps :code:`create_manifests`, which in turn wraps two other tools, :code:`combine_route_tables` and :code:`format_combined_routes` into one tool. You can still use any of those tools if you wish, but you can instead just use :code:`build_routes_from_chunked`. Calling these intermediate tools from :code:`build_routes_from_chunked` wasn't necessary, but it was convenient to develop that way and has the added benefit of producing intermediate files at each step that you can use if you need to revert to the old method for some of the steps for some reason (say there was an error in one of the route optimizations and you want to retry it without running all of them again).

.. mermaid::
    :caption: Subtools wrapped and alternatively available for use

    graph TD;
        A[**build_routes_from_chunked**] --> B[**split_chunked_route**]
        B --> B1[Splits master chunked sheet into individual driver route sheets.]
        A --> C[Uploads routes to Circuit, optimizes, and distributes to drivers.]
        A --> D[**create_manifests_from_circuit**]
        D --> E[Gets routes from Circuit.]
        D --> F[**create_manifests**]
        F --> G[**combine_route_tables**]
        F --> H[**format_combined_routes**]
        G --> G1[Combines downloaded routes CSVs into a single workbook.]
        H --> H1[Formats the combined routes into printable manifests.]

For instance, say you've found a bug when using :code:`build_routes_from_chunked` where the routes uploaded and optimized but for some reason didn't produce the final printable manifest. You could try downloading the routes manually then running :code:`create_manifests`, or downloading manually then running :code:`combine_route_tables` and passing its output to :code:`format_combined_routes`. For whichever of those steps fails, you can revert to using your old method, but you can still ostensibly use a tool for the other piece that didn't fail. For example, say :code:`combine_route_tables` ran fine, but :code:`format_combined_routes` threw an error, so you reverted to using the old Excel macro and manually formatting. See :doc:`split_chunked_route`, :doc:`create_manifests_from_circuit`, :doc:`create_manifests`, :doc:`combine_route_tables` and :doc:`format_combined_routes`.

Most likely you'll find that the tool works fine unless the underlying data schemata have changed, but it's good to know you have options to explore instead of doing it all manually again.

See Also
--------

:doc:`workflow`

:doc:`split_chunked_route`

:doc:`create_manifests_from_circuit`

:doc:`create_manifests`

:doc:`combine_route_tables`

:doc:`format_combined_routes`

:doc:`CLI`

:doc:`bfb_delivery.api`