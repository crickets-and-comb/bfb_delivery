================
Format Manifests
================

After you've combined your optimized routes into a single workbook, using :code:`combine_route_tables` (see :doc:`combine_route_tables </combine_route_tables>`), you'll need to format the combined routes into printable manifests for the drivers. This can be done using the :code:`format_combined_routes` tool.

This tool replaces the Excel macro previously used, as well as some manual steps afterward. The output is ready to print.

.. note::

    :code:`create_manifests` wraps this tool and :code:`combine_route_tables` into one tool. You can still use them if you wish, but you can instead use :code:`create_manifests`. See :doc:`create_manifests </create_manifests>` and :doc:`combine_route_tables </combine_route_tables>`.

Python API documentation at :py:func:`bfb_delivery.api.public.format_combined_routes`.

CLI documentation at :doc:`CLI </CLI>`.

Setup
-----

This tool requires a local config file with phone numbers. We store them locally so we don't put phone numbers in the codebase. The config file should be named ``config.ini`` and should be in the same directory as where you're running the tool. The file should look like this:

.. code:: ini

    [phone_numbers]
    driver_support = 555-555-5555
    recipient_support = 555-555-5555 x5

Usage
-----

When you ran :code:`combine_route_tables`, you received a single workbook with all the optimized routes combined. Now, you can pass that file to :code:`format_combined_routes`, along with any other optional arguments, to create printable manifests for your drivers. If you ran the CLI, the filepath to the combined file was printed to the console. If you used the Python API, the function will return the filepath to the formatted file.

You must at least pass :code:`input_path` to :code:`format_combined_routes`:

.. code:: python

    from bfb_delivery import format_combined_routes

    combine_route_tables(input_path="path/to/combined_workbook.xlsx")

Or, use the CLI:

.. code:: bash

    combine_route_tables --input_path path/to/combined_workbook.xlsx


The function will return the filepath to the formatted manifest workbook, which you can then open, review, and print. If you're using the CLI, the filepath will be printed to the console.

.. note::
    
    You can pass the :code:`combine_route_tables` ouput to :code:`format_combined_routes` without reviewing the combined file first. We're going to soon wrap these two steps into a single tool. But, for now, you need to run them separately.

Optional arguments
^^^^^^^^^^^^^^^^^^

You can specify a few things about the formatted manifest workbook.

Output directory
~~~~~~~~~~~~~~~~

Use the optional argument :code:`output_dir` to specify the filepath where the combined file will be saved:

.. code:: python

    combine_route_tables(
        input_path="path/to/combined_workbook.xlsx",
        output_dir="path/to/output_dir/",
    )

.. code:: bash

    combine_route_tables --input_path path/to/combined_workbook.xlsx --output_dir path/to/output_dir

Output filename
~~~~~~~~~~~~~~~

Choose the filename with :code:`output_name`. The default filename will be :code:`combined_routes_{today's date}.xlsx` (e.g., :code:`combined_routes_19991231.xlsx`). But, you can pass a preferred name that will be used instead:

.. code:: python

    combine_route_tables(
        input_path="path/to/combined_workbook.xlsx",
        output_name="manifests.xlsx",
    )

.. code:: bash

    combine_route_tables --input_path path/to/combined_workbook.xlsx --output_name manifests.xlsx

Manifest date
~~~~~~~~~~~~~

A date is prepended to the driver's name in the manifest worksheets, and it's also used in the date field in the worksheets.

The default is today's date as ``MM.DD``. But, you can pass a specific date with :code:`date`:

.. code:: python

    combine_route_tables(
        input_path="path/to/combined_workbook.xlsx",
        date="1971.01.27",
    )

.. code:: bash

    combine_route_tables --input_path path/to/combined_workbook.xlsx --date "1971.01.27"

It doesn't have to be a date; it can be any text you want. Also, it doesn't affect the date in the filename.
