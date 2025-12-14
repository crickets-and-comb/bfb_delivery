:orphan:

================
Format Manifests
================

After you've combined your optimized routes into a single workbook, using :code:`combine_route_tables` (see :doc:`combine_route_tables`), you'll need to format the combined routes into printable manifests for the drivers. This can be done using the :code:`format_combined_routes` tool.

This tool replaces the Excel macro previously used, as well as some manual steps afterward. The output is ready to print.

.. note::

    Uses the date of the front of each CSV name to set the manifest date field.
    I.e., each sheet should be named something like "02.12 Bill C",
    and, e.g., this would set the manifest date field to "Date: 02.12".

.. note::

    :code:`create_manifests` wraps this tool and :code:`combine_route_tables` into one tool. You can still use them if you wish, but you can instead use :code:`create_manifests`. See :doc:`create_manifests` and :doc:`combine_route_tables`.

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

You must at least pass :code:`input_path` to :code:`format_combined_routes`.

In Python:

.. code:: python

    from bfb_delivery import format_combined_routes

    format_route_table(input_path="path/to/combined_workbook.xlsx")

With CLI:

.. code:: bash

    format_route_table --input_path path/to/combined_workbook.xlsx


The function will return the filepath to the formatted manifest workbook, which you can then open, review, and print. If you're using the CLI, the filepath will be printed to the console.

.. note::
    
    You can pass the :code:`combine_route_tables` ouput to :code:`format_combined_routes` without reviewing the combined file first. We're going to soon wrap these two steps into a single tool. But, for now, you need to run them separately.

Optional arguments
^^^^^^^^^^^^^^^^^^

You can specify a few things about the formatted manifest workbook. Use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    format_route_table --help

Output directory
~~~~~~~~~~~~~~~~

Use the optional argument :code:`output_dir` to specify the filepath where the combined file will be saved.

In Python:

.. code:: python

    format_route_table(
        input_path="path/to/combined_workbook.xlsx",
        output_dir="path/to/output_dir/",
    )

With CLI:

.. code:: bash

    format_route_table --input_path path/to/combined_workbook.xlsx --output_dir path/to/output_dir

Output filename
~~~~~~~~~~~~~~~

Choose the filename with :code:`output_name`. The default filename will be :code:`combined_routes_{today's date}.xlsx` (e.g., :code:`combined_routes_19991231.xlsx`). But, you can pass a preferred name that will be used instead.

In Python:

.. code:: python

    format_route_table(
        input_path="path/to/combined_workbook.xlsx",
        output_name="manifests.xlsx",
    )

With CLI:

.. code:: bash

    format_route_table --input_path path/to/combined_workbook.xlsx --output_name manifests.xlsx


Supplying extra notes
~~~~~~~~~~~~~~~~~~~~~

Use the optional argument :code:`extra_notes_file` to specify a CSV file with extra notes to include in the manifest. The CSV file should have two columns: :code:`tag` and :code:`note`. The tag is the text (usually asterisked) that appears in the standard notes field for a delivery. The note is then added to the bottom of the manifest with the tag. For example:

.. code-block:: text

    tag,note
    Cedarwood Apartments*,Please call the recipient when you arrive.

This file will put the note "Please call the recipient when you arrive." at the bottom of the manifest (once) if a stop has a note that contains the text "Cedarwood Apartments special instructions \*".

If :code:`extra_notes_file` is not provided, the tool will use the constant notes in the codebase: :py:data:`bfb_delivery.lib.constants.ExtraNotes`.

In Python:

.. code:: python

    format_route_table(
        input_path="path/to/combined_workbook.xlsx",
        extra_notes_file="path/to/extra_notes.csv",
    )

With CLI:

.. code:: bash

    format_route_table --input_path path/to/combined_workbook.xlsx --extra_notes_file path/to/extra_notes.csv

.. note::

    Extra notes are placed in merged cells with automatic row height calculation. The height calculation is approximate and may not be perfect for all text lengths and formatting. Manual review of the cell heights is recommended to ensure notes are fully visible.


See Also
--------

:doc:`create_manifests`

:doc:`workflow`

:doc:`combine_route_tables`

:doc:`CLI`

:doc:`bfb_delivery.api`