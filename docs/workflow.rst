==============================
The Delivery-Planning Workflow
==============================

Once you're set up (see :doc:`getting_started`), and you have a master list of chunked routes you want to split up and optimize in Circuit, and you've activated the selected drivers in Circuit, you can begin using :code:`bfb_delivery`. Here's the workflow.

.. mermaid::
   :caption: Delivery Planning Workflow

   graph TD;
       activate_all_hhs_driver["Activate 'All HHs' driver in Circuit."];
       activate_env[Activate your conda env.];
       activate_all_hhs_driver --> get_long_route[Use **build_routes_from_chunked** to route all stops with Circuit.];
       activate_env --> get_long_route;
       get_long_route --> chunk["'Chunk' stops by driver."];
       get_long_route --> activate_drivers[Activate drivers in Circuit.];
       chunk --> build_routes_from_chunked[Use **build_routes_from_chunked** to run routes through Circuit and create manifests.];
       activate_drivers --> build_routes_from_chunked;
       build_routes_from_chunked --> review_output[Review the console output to confirm success.];
       build_routes_from_chunked --> print[Review and print the manifests for your drivers.];

Here's the workflow in more detail.

Activate your env
-----------------

Before you can use the ``bfb_delivery`` package and tools, namely :code:`build_routes_from_chunked`, you need to activate the :code:`bfb_delivery` conda environment. (You may have named your env something else.)

First, open your terminal (e.g., Anaconda Command Prompt), navigate to the correct directory (the one with your ``config.ini`` and ``.env`` files you set up in :doc:`getting_started`), and activate your env:

.. code:: bash

    cd path/to/bfb_delivery/dir
    conda activate my_bfb_delivery_env_name

See :doc:`getting_started` for more information.

Now you're ready to use the tools in the ``bfb_delivery`` command-line interface (CLI).

Route all stops with Circuit
----------------------------

When you have all the stops for the week, you need to upload them to Circuit and optimize them in a single route with the title "All HHs". You'll use this single route as a starting point to allocate stops to drivers.

You can build the single route manually, or you can use :code:`build_routes_from_chunked` to upload the stops to Circuit and optimize the route for you. Just put "All HHs" in the driver column for all stops in the routes spreadsheet, activate the All HHs driver (or whatever driver you want to assign it to), then run the tool:

.. code:: bash

    build_routes_from_chunked --input_path "path/to/all_hhs.xlsx"

The tool will prompt you to confirm the driver assignment. See :doc:`build_routes_from_chunked` for an example, as well as e.g. for how to set the output directory. You may also use `--help` to see all the optional arguments in the CLI:

.. code:: bash

    build_routes_from_chunked --help

The filepath to the *combined workbook* will print to the console; ignore the filepath to the *final manifest*. You will not use it at this step; it's the main output of the tool when you use it later to make the final manifests for the true routes, but the *combined workbook* is a byproduct of that process and the one you need now.

Chunk the stops by driver
-------------------------

From the single optimized route of several hundred stops, you'll next allocate them to each of your roughly 40 drivers. This is a complicated hooman task. You're the hooman. Get a snack or three, stay hydrated, and have a blast. You're gonna love it.

.. note::

    When assignng more that one route to a driver, use the following convention instead of the driver's name: "Driver Name #1", "Driver Name #2", etc. This will tell the tool to split the routes instead of making one long route for the driver.

Activate your env
-----------------

Phewf! You have your chunked routes. Now you need to upload the routes to Circuit, optimize them, distribute them to the driver apps (if you wish), and build the final manifests to print for the drivers. You can do this with :code:`build_routes_from_chunked`.

.. code:: bash

    build_routes_from_chunked --input_path "path/to/master_chunked.xlsx"

The tool will prompt you to confirm the driver assignments. See :doc:`build_routes_from_chunked` for an example, as well as e.g. for how to set the output directory. You may also use `--help` to see all the optional arguments in the CLI:

.. code:: bash

    build_routes_from_chunked --help

Once you've confirmed the driver assignments, allow the tool to run up to a minute per route (about 30 minutes). When finished, the tool will print the filepath to the final manifest Excel workbook, which you will print for your drivers.

Review the console output to confirm success
--------------------------------------------

Review the console output to confirm that the routes were uploaded to Circuit, optimized, and distributed to the drivers. If there are any errors, you'll see them here. See :doc:`build_routes_from_chunked` for what to look for.

Review and print manifests
--------------------------

Finally, review and print the manifests for your drivers.


See Also
--------

:doc:`getting_started`

:doc:`build_routes_from_chunked`

:doc:`CLI`

:doc:`bfb_delivery.api`