================================================
Create Printable Manifests from Optimized Routes
================================================

After you have optimized each driver's route in Circuit, you will need to combine the optimized routes back into a single workbook of driver manifest sheets ready to print. You can do this with the :code:`create_manifests` tool.

This tool replaces the manual task of copying each driver's optimized route into a single workbook, running an Excel macro, and finishing up with some manual steps. It will combine all the optimized routes into a single workbook, with each driver's route on a separate sheet, and format the sheets into manifests ready to print.

.. note::

    This wraps the two other tools :code:`combine_route_tables` and :code:`format_combined_routes` into one tool. You can still use them if you wish, but you can instead use this tool. For instance, say you've found a bug when using :code:`create_manifests`. You could try running :code:`combine_route_tables` then passing its output to :code:`format_combined_routes`. For whichever of those steps fails you can revert to using your old method, but you can still ostensibly use the tool for the other piece that didn't fail (e.g., :code:`combine_route_tables` ran fine, but :code:`format_combined_routes` threw an error, so you reverted to using the Excel macro and manually formatting). See :doc:`combine_route_tables` and :doc:`format_combined_routes`.

Python API documentation at :py:func:`bfb_delivery.api.public.create_manifests`.

CLI documentation at :doc:`CLI`.

Usage
-----

You pass the directory containing the optimized route tables to :code:`create_manifests`, along with any other optional arguments, and it will create a single workbook file with all the optimized routes formatted and ready to print. The tool then returns the filepath to that file.

.. note::

    The route CSVs from Circuit should be in a single directory, with no other CSVs in it.

.. note::

    This will change the "Product Type" column name, per Circuit API, back to "Box Type" per food bank staff preferences.

You must at least pass :code:`input_dir` to :code:`create_manifests`:

.. code:: python

    from bfb_delivery import create_manifests

    create_manifests(input_dir="path/to/input/")

Or, use the command-line-interface:

.. code:: bash

    create_manifests --input_dir path/to/input/

The function will return the filepath to the combined file. If you're using the CLI, the filepath will be printed to the console.

Optional arguments
^^^^^^^^^^^^^^^^^^

You can use optional arguments specify a few things about the manifest workbook.

Output directory
~~~~~~~~~~~~~~~~

Use the optional argument :code:`output_dir` to specify the directory where the workbook file will be saved:

.. code:: python

    create_manifests(input_dir="path/to/input/", output_dir="path/to/output_dir/")

.. code:: bash

    create_manifests --input_dir path/to/input/ --output_dir path/to/output_dir/

Output filename
~~~~~~~~~~~~~~~

Choose the filename with :code:`output_name`. The default filename will be :code:`final_manifests_{today's date}.xlsx` (e.g., :code:`final_manifests_19991231.xlsx`). But, you can pass a preferred name instead:

.. code:: python

    create_manifests(input_dir="path/to/input/", output_name="all_routes.xlsx")

.. code:: bash

    create_manifests --input_dir path/to/input/ --output_name all_routes.xlsx

.. note::

    You can use both `output_dir` and `output_name` together to specify the directory and filename of the output workbook.

Manifest date
~~~~~~~~~~~~~

A date is prepended to the driver's name in the manifest worksheets, and it's also used in the date field in the worksheets.

The default is today's date as ``MM.DD``. But, you can pass a specific date with :code:`date`:

.. code:: python

    create_manifests(
        input_dir="path/to/input/",
        date="1971.01.27",
    )

.. code:: bash

    create_manifests --input_dir path/to/input/ --date "1971.01.27"

It doesn't have to be a date; it can be any text you want. Also, it doesn't affect the date in the filename.


Supplying extra notes
~~~~~~~~~~~~~~~~~~~~~

Use the optional argument :code:`extra_notes_file` to specify a CSV file with extra notes to include in the manifest. The CSV file should have two columns: :code:`tag` and :code:`note`. The tag is the text (usually asterisked) that appears in the standard notes field for a delivery. The note is then added to the bottom of the manifest with the tag. For example:

.. code:: csv

    tag,note
    Cedarwood Apartments special instructions *,Please call the recipient when you arrive.

This file will put the note "Please call the recipient when you arrive." at the bottom of the manifest (once) if a stop has a note that contains the text "Cedarwood Apartments special instructions \*".

If :code:`extra_notes_file` is not provided, the tool will use the constant notes in the codebase: :py:data:`bfb_delivery.lib.constants.ExtraNotes`

.. code:: python

    create_manifests(
        input_dir="path/to/combined_workbook.xlsx",
        extra_notes_file="path/to/extra_notes.csv",
    )

.. code:: bash

    create_manifests --input_dir path/to/combined_workbook.xlsx --extra_notes_file path/to/extra_notes.csv


See Also
--------

:doc:`workflow`

:doc:`combine_route_tables`

:doc:`format_combined_routes`

:doc:`CLI`

:doc:`bfb_delivery.api`