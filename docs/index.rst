============================
BFB Delivery: A Cruft Cutter
============================

Library and CLI for Bellingham Food Delivery. This doesn't do much yet.

So far, you can:

    1. Split a driver-labeled route spreadsheet into individual driver spreadsheets. See :doc:`split_chunked_route` for more information.
    2. Combine driver route spreadsheets into a single workbook. See :doc:`combine_route_tables` for more information.

Contents
--------

.. toctree::
   :maxdepth: 3

   combine_route_tables
   split_chunked_route
   API documentation <modules>

Usage examples
--------------


Library
^^^^^^^

Avoid calling library functions directly and stick to the public API:

.. code:: python

    from bfb_delivery import split_chunked_route
    # These are okay too:
    # from bfb_delivery.api import split_chunked_route
    # from bfb_delivery.api.public import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")

If you're a power user or just like feeling like one, you can use the internal API:

.. code:: python

    from bfb_delivery.api.internal import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")


Nothing is stopping you from importing from lib directly, but you should avoid it unless you like to tell people, "Danger is my middle name.":

.. code:: python

    from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")


CLI
^^^

You can use the command-line-interface if you have this package installed in your environment:

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx