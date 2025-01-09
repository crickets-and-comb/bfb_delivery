=============================================
Split Chunked Route Sheet into Multiple Files
=============================================

Once you have a large single-route sheet labeled by driver, representing individual driver routes, you need to split it into multiple files and sheets to then upload each driver's route to Circuit for final optimization. This splitting task can be done by using the :code:`split_chunked_route` tool.

This tool replaces the manual task of splitting the single route grouped by driver into individual Excel worksheets for each driver, grouped into a handful of workbooks for each staff member to upload to Circuit.

Python API documentation at :py:func:`bfb_delivery.api.public.split_chunked_route`.

CLI documentation at :doc:`CLI`.

Usage
-----

You pass the filepath of the large route sheet to :code:`split_chunked_route`, along with any other optional arguments, and it will create workbook files split up for each staff member working on the route, with each sheet containing the deliveries for a single driver. The tool then returns the filepaths to those files so you can upload them to Circuit.

.. note::

    This will change the "Box Type" column name to "Product Type" to match the Circuit API.

You must at least pass :code:`input_path` to :code:`split_chunked_route`:

.. code:: python

    from bfb_delivery import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")

Or, use the command-line-interface:

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx

The function will return a list of filepaths to the split files, which you can then upload to Circuit. If you're using the CLI, the filepaths will be printed to the console.

Optional arguments
^^^^^^^^^^^^^^^^^^

You can specify a few things about the split files.

Number of workbooks
~~~~~~~~~~~~~~~~~~~

You can optionally specify how many workbooks you want to split the route into by passing the argument :code:`n_books`. The default is 4, meaning the route will be split into 4 workbooks, each with a unique set of driver routes, for 4 staff members to upload.

.. code:: python

    split_chunked_route(input_path="path/to/input.xlsx", n_books=3)

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx --n_books 3

Output directory
~~~~~~~~~~~~~~~~

Use the optional argument :code:`output_dir` to specify the directory where the split files will be saved:

.. code:: python

    split_chunked_route(input_path="path/to/input.xlsx", output_dir="path/to/output")

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx --output_dir path/to/output

Output filename
~~~~~~~~~~~~~~~

Use :code:`output_name` to choose a standard filename. The default filenames will be :code:`split_workbook_{today's date}_{i of n workbooks}.xlsx` (e.g., :code:`split_workbook_19991231_1_of_3.xlsx`). But, you can pass a preferred name that will be used instead, with just the workbook number appended to it. So, passing :code:`output_name` as :code:`driver_routes` will result in files named :code:`driver_routes_1.xlsx`, :code:`driver_routes_2.xlsx`, etc.

.. code:: python

    split_chunked_route(input_path="path/to/input.xlsx", output_name="driver_routes")

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx --output_name driver_routes


Book-one drivers
~~~~~~~~~~~~~~~~

By default, the first workbook will include the drivers listed in the constant :py:data:`bfb_delivery.lib.constants.BookOneDrivers`.

Use :code:`book_one_drivers_file` to specify a CSV file of drivers that should be in the first workbook instead. This is useful if you need to change the drivers that need to be in the first workbook but the update hasn't been released yet.

The CSV should be a single column with the header "Driver", like this:

.. code:: csv

    Driver
    Alice S
    Bob T
    Charlie U

.. code:: python

    split_chunked_route(
        input_path="path/to/input.xlsx",
        book_one_drivers_file="path/to/book_one_drivers.csv"
    )

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx --book_one_drivers_file path/to/book_one_drivers.csv

.. note::

    This argument and the default won't work correctly if you have too many drivers for the first workbook. The tool simply bumps those drivers to the top of the list and then splits all drivers evenly between workbooks. For example, if you have 100 drivers, 4 workbooks to make, and 30 book-one drivers, only the first 25 of those book-one drivers will go to book one, and the remaining 5 will go to book two. If this is a problem, please request a fix.


See Also
--------

:doc:`workflow`

:doc:`CLI`

:doc:`bfb_delivery.api`