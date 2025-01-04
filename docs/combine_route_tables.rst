==================================================
Combine Driver Route Tables into a Single Workbook
==================================================

After you have optimized each driver's route in Circuit, you will need to combine the optimized routes back into a single workbook to create the driver manifest. This combining task can be done by using the :code:`combine_route_tables` tool.

This tool replaces the manual task of copying each driver's optimized route into a single workbook. It will combine all the optimized routes into a single workbook, with each driver's route on a separate sheet.

(After this step you'll pass the combined workbook to :code:`format_combined_routes` to create the printable manifest. See :doc:`format_combined_routes </format_combined_routes>`.)

.. note::

    :code:`create_manifests` wraps this tool and :code:`format_combined_routes` into one tool. You can still use them if you wish, but you can instead use :code:`create_manifests`. See :doc:`create_manifests </create_manifests>` and :doc:`format_combined_routes </format_combined_routes>`.

Python API documentation at :py:func:`bfb_delivery.api.public.combine_route_tables`.

CLI documentation at :doc:`CLI </CLI>`.

Usage
-----

You pass the directory containing the optimized route tables to :code:`combine_route_tables`, along with any other optional arguments, and it will create a single workbook file with all the optimized routes combined. The tool then returns the filepath to that file so you can continue to work with it as needed (formatting and printing, see :doc:`format_combined_routes </format_combined_routes>`).

.. note::

    The route CSVs from Circuit should be in a single directory, with no other CSVs in it.

.. note::

    This will change the "Product Type" column name, per Circuit API, back to "Box Type" per food bank staff preferences.

You must at least pass :code:`input_dir` to :code:`combine_route_tables`:

.. code:: python

    from bfb_delivery import combine_route_tables

    combine_route_tables(input_dir="path/to/input/")

Or, use the command-line-interface:

.. code:: bash

    combine_route_tables --input_dir path/to/input/

The function will return the filepath to the combined file, which you can then open and review before you pass to :code:`format_combined_routes` to format the manifests for printing (see :doc:`format_combined_routes </format_combined_routes>`). If you're using the CLI, the filepath will be printed to the console.

.. note::
    
    You can pass the :code:`combine_route_tables` ouput to :code:`format_combined_routes` without reviewing the combined file first. We're going to soon wrap these two steps into a single tool. But, for now, you need to run them separately.

Optional arguments
^^^^^^^^^^^^^^^^^^

Use the optional argument :code:`output_dir` to specify the directory where the workbook file will be saved:

.. code:: python

    combine_route_tables(input_dir="path/to/input/", output_dir="path/to/output_dir/")

.. code:: bash

    combine_route_tables --input_dir path/to/input/ --output_dir path/to/output_dir/

Choose the filename with :code:`output_name`. The default filename will be :code:`combined_routes_{today's date}.xlsx` (e.g., :code:`combined_routes_19991231.xlsx`). But, you can pass a preferred name that will be used instead:

.. code:: python

    combine_route_tables(input_dir="path/to/input/", output_name="all_routes.xlsx")

.. code:: bash

    combine_route_tables --input_dir path/to/input/ --output_name all_routes.xlsx
