============================
BFB Delivery: A Cruft Cutter
============================

Library and CLI for Bellingham Food Bank Delivery.

Use this package to cut some cruft around creating delivery route manifests for the Bellingham Food Bank. There is one main tool :doc:`build_routes_from_chunked`, which is a CLI tool to take a spreadsheet of stops assigned to drivers ("chunked" routes, as staff call it) and upload to Circuit to build, optimize, and distribute routes to drivers, producing also a final manifest spreadsheet to print for each driver.

Now that staff have adopted this tool, they estimate it saves about five paid staff hours per week, and reduces the room for error in the process.


Contents
--------

.. toctree::
   :maxdepth: 3

   getting_started
   workflow
   build_routes_from_chunked
   intro_to_bash
   CLI
   modules
   developers

Installation
------------

Run the following to install the package:

.. code:: bash

    pip install bfb_delivery

See :doc:`getting_started` for more information.

See Also
--------

`BFB Delivery GitHub repository <https://github.com/crickets-and-comb/bfb_delivery/>`_

`bfb-delivery PyPi distribution <https://pypi.org/project/bfb-delivery/>`_