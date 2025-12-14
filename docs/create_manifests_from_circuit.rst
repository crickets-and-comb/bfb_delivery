:orphan:

=============================================================
Create Printable Manifests from Optimized Routes from Circuit
=============================================================

After you have optimized each driver's route in Circuit, you will need to combine the optimized routes back into a single workbook of driver manifests ready to print, with headers, aggregate data, and color-coded box types. You can do this with the :code:`create_manifests_from_circuit` tool.

This tool replaces the manual task of downloading the optimized routes, copying each route into a single workbook, running an Excel macro, and finishing up with some manual steps. Instead, :code:`create_manifests_from_circuit` will download and combine all the optimized routes into a single workbook, with each driver's route on a separate sheet, and format the sheets into manifests ready to print.

.. note::

    Uses the date of the front of each route's title to set the manifest date field.
    I.e., each route should be titled something like "07.17 Walt D",
    and, e.g., this would set the manifest date field to "Date: 07.17".

Python API documentation at :py:func:`bfb_delivery.api.public.create_manifests_from_circuit`.

CLI documentation at :doc:`CLI`.

Usage
-----

You call :code:`create_manifests_from_circuit` with (optionally) the start date of the routes, along with any other optional arguments, and it will create a single workbook file with all the optimized routes formatted and ready to print. The tool then returns the filepath to that file.

.. note::

    The filepaths to some intermediate files will print to the console as well, before finally printing the filepath to the final workbook. In addition to the final manifest, there will route CSVs in a directory and an unformatted workbook of all the routes.

.. note::

    This will change the "Product Type" column name, per Circuit API, back to "Box Type" per food bank staff preferences.

In Python:

.. code:: python

    from bfb_delivery import create_manifests_from_circuit

    create_manifests_from_circuit(start_date="1903-12-17")

With CLI:

.. code:: bash

    create_manifests_from_circuit --start_date 1919-06-14

The function will return the filepath to the final manifest. If you're using the CLI, the filepath will print to the console.

.. note::

    This takes about a minute to run as it downloads the routes from Circuit.

.. note::

    You don't need to pass in :code:`start_date`. If you don't pass it in, the tool will assume the soonest Friday.

Optional arguments
^^^^^^^^^^^^^^^^^^

You can use optional arguments to specify a few things about the manifest workbook. Use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    create_manifests_from_circuit --help

Start date
~~~~~~~~~~

Use the optional argument :code:`start_date` to specify the beginning of the date range to search Circuit for routes. The default if not passed in is the soonest Friday.

.. code:: python

    from bfb_delivery import create_manifests_from_circuit

    create_manifests_from_circuit(start_date="1947-10-14")

With CLI:

.. code:: bash

    create_manifests_from_circuit --start_date 1957-10-04

End date
~~~~~~~~

Use the optional argument :code:`end_date` to specify the end of the date range to search Circuit for routes. The default is the start date.

.. code:: python

    from bfb_delivery import create_manifests_from_circuit

    create_manifests_from_circuit(end_date="1961-04-12")

With CLI:

.. code:: bash

    create_manifests_from_circuit --end_date 1969-07-20


Output directory
~~~~~~~~~~~~~~~~

Use the optional argument :code:`output_dir` to specify the directory to save the workbook file in.

In Python:

.. code:: python

    create_manifests_from_circuit(output_dir="path/to/output_dir/")

With CLI:

.. code:: bash

    create_manifests_from_circuit --output_dir path/to/output_dir/

Output filename
~~~~~~~~~~~~~~~

Choose the filename with :code:`output_name`. The default filename will be :code:`final_manifests_{today's date}.xlsx` (e.g., :code:`final_manifests_19991231.xlsx`). But, you can pass a preferred name instead.

In Python:

.. code:: python

    create_manifests_from_circuit(output_name="all_routes.xlsx")

With CLI:

.. code:: bash

    create_manifests_from_circuit --output_name all_routes.xlsx

.. note::

    You can use both `output_dir` and `output_name` together to specify the directory and filename of the output workbook.

Circuit output directory
~~~~~~~~~~~~~~~~~~~~~~~~

Use the optional argument :code:`circuit_output_dir` to specify the directory in which to save the route CSVs downloaded from Circuit.

In Python:

.. code:: python

    create_manifests_from_circuit(circuit_output_dir="path/to/circuit_output_dir/")

With CLI:

.. code:: bash

    create_manifests_from_circuit --circuit_output_dir path/to/circuit_output_dir/


All HHs
~~~~~~~

If you want to get the "All HHs" route that was optimized as a single route before chunking into driver routes, use the optional argument :code:`all_hhs`.

In Python:

.. code:: python

    create_manifests_from_circuit(all_hhs=True)

With CLI:

.. code:: bash

    create_manifests_from_circuit --all_hhs

.. note::

    If you're using this, you're not likely using it to create a final manifest, but rather a plain spreadsheet to start chunking into separate routes. So, you'll want look in the console for the filepath to the *combined workbook*, not the final manifest.

Verbose output
~~~~~~~~~~~~~~

Use the optional argument :code:`verbose` to print more information to the console.

In Python:

.. code:: python

    create_manifests_from_circuit(verbose=True)

With CLI:

.. code:: bash

    create_manifests_from_circuit --verbose

Supplying extra notes
~~~~~~~~~~~~~~~~~~~~~

Use the optional argument :code:`extra_notes_file` to specify a CSV file with extra notes to include in the manifest. The CSV file should have two columns: :code:`tag` and :code:`note`. The tag is the text (usually asterisked) that appears in the standard notes field for a delivery. The note is then added to the bottom of the manifest with the tag. For example:

.. code-block:: text

    tag,note
    Cedarwood Apartments special instructions *,Please call the recipient when you arrive.

This file will put the note "Please call the recipient when you arrive." at the bottom of the manifest (once) if any stops have a note that contains the text "Cedarwood Apartments special instructions \*".

If you don't provide :code:`extra_notes_file` provide, the tool will use the constant notes in the codebase: :py:data:`bfb_delivery.lib.constants.ExtraNotes` (currently empty).

In Python:

.. code:: python

    create_manifests_from_circuit(extra_notes_file="path/to/extra_notes.csv")

With CLI:

.. code:: bash

    create_manifests_from_circuit --extra_notes_file path/to/extra_notes.csv

.. note::

    Extra notes are placed in merged cells with automatic row height calculation. The height calculation is approximate and may not be perfect for all text lengths and formatting. Manual review of the cell heights is recommended to ensure notes are fully visible.

Note on tools this tool wraps
-----------------------------

:code:`create_manifests_from_circuit` wraps another tool, :code:`create_manifests`, so you don't have to download and move files around. :code:`create_manifests` in turn wraps two other tools, :code:`combine_route_tables` and :code:`format_combined_routes` into one tool. You can still use any of those tools if you wish, but you can instead just use :code:`create_manifests_from_circuit`.

.. mermaid::
    :caption: Subtools wrapped and alternatively available for use

    graph TD;
        A[**create_manifests_from_circuit**] --> B[Gets routes from Circuit]
        A --> C[**create_manifests**]
        C --> D[**combine_route_tables**]
        C --> E[**format_combined_routes**]

For instance, say you've found a bug when using :code:`create_manifests_from_circuit`. You could try downloading the routes manually and running :code:`create_manifests`, or running :code:`combine_route_tables` and passing its output to :code:`format_combined_routes`. For whichever of those steps fails you can revert to using your old method, but you can still ostensibly use the tool for the other piece that didn't fail. For example, say :code:`combine_route_tables` ran fine, but :code:`format_combined_routes` threw an error, so you reverted to using the old Excel macro and manually formatting. See :doc:`create_manifests`, :doc:`combine_route_tables` and :doc:`format_combined_routes`.

Most likely you'll find that the tool works fine unless the underlying data schemata have changed, but it's good to know you have options to explore instead of doing it all manually again.

See Also
--------

:doc:`workflow`

:doc:`create_manifests`

:doc:`combine_route_tables`

:doc:`format_combined_routes`

:doc:`CLI`

:doc:`bfb_delivery.api`