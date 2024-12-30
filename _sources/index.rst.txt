============================
BFB Delivery: A Cruft Cutter
============================

Library and CLI for Bellingham Food Bank Delivery. See also the GitHub repository: https://github.com/crickets-and-comb/bfb_delivery

So far, you can use this package to cut some cruft around creating delivery route manifests. You can:

    1. Split a driver-labeled route spreadsheet into individual driver spreadsheets. See :doc:`split_chunked_route` for more information.
    2. Combine driver route spreadsheets into a single workbook. See :doc:`combine_route_tables` for more information.
    3. Format the combined route workbook into manifests for printing. See :doc:`format_combined_routes` for more information.


Contents
--------

.. toctree::
   :maxdepth: 3

   getting_started
   split_chunked_route
   combine_route_tables
   format_combined_routes
   CLI
   modules
   developers

Installation
------------

Run the following to install the package:

.. code:: bash

    pip install bfb_delivery

See :doc:`getting_started` for more information.

See also https://pypi.org/project/bfb-delivery/.