==================================================
Combine Driver Route Tables into a Single Workbook
==================================================

After you have optimized each driver's route in Circuit, you will need to combine the optimized routes back into a single workbook to share with your team. This combining task can be done by using the :code:`combine_route_tables` tool. (API documentation at :py:func:`bfb_delivery.api.public.combine_route_tables`.)

Usage
-----

You pass the filepaths of the optimized route tables to :code:`combine_route_tables`, along with any other optional arguments, and it will create a single workbook file with all the optimized routes combined. The tool then returns the filepath to that file so you can continue to work with it as needed (formatting and printing).

You must at least pass :code:`input_paths` to :code:`combine_route_tables`:

.. code:: python

    from bfb_delivery import combine_route_tables

    combine_route_tables(input_paths=["path/to/input1.xlsx", "path/to/input2.xlsx"])

Or, use the command-line-interface:

.. code:: bash

    combine_route_tables --input_paths path/to/input1.xlsx --input_paths path/to/input2.xlsx

Note, for the CLI, you currently pass :code:`--input_paths` multiple times to specify multiple input files. But, if desired, we'll soon allow you to pass the path to a file that lists the paths to the input files.

The function will return the filepath to the combined file, which you can then open, format, and work with as needed. If you're using the CLI, the filepath will be printed to the console.

Optional arguments
^^^^^^^^^^^^^^^^^^

Use the optional argument :code:`--output_path` to specify the filepath where the combined file will be saved:

.. code:: python

    combine_route_tables(
        input_paths=["path/to/input1.xlsx", "path/to/input2.xlsx"],
        output_path="path/to/output.xlsx",
    )

.. code:: bash

    combine_route_tables --input_paths path/to/input1.xlsx --input_paths path/to/input2.xlsx --output_path path/to/output.xlsx

The default filename will be :code:`combined_routes_{today's date}.xlsx` (e.g., :code:`combined_routes_2021-01-01.xlsx`). But, you can pass a preferred name that will be used instead:

.. code:: python

    combine_route_tables(
        input_paths=["path/to/input1.xlsx", "path/to/input2.xlsx"],
        output_name="all_routes",
    )

.. code:: bash

    combine_route_tables --input_paths path/to/input1.xlsx --input_paths path/to/input2.xlsx --output_name all_routes