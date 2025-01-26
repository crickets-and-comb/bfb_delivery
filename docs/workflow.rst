==============================
The Delivery-Planning Workflow
==============================

Once you're set up (see :doc:`getting_started`), and you have a master list of chunked routes you want to split up and optimize in Circuit, you can begin using :code:`bfb_delivery`. Here's the workflow.

.. mermaid::
   :caption: Delivery Planning Workflow

   graph TD;
       get_long_route[Route all stops with Circuit.] --> chunk["Chunk" stops by driver.];
       chunk --> activate_env[Activate your env.];
       activate_env --> split_chunked_route[Use **split_chunked_route** to create the workbooks for upload to Circuit.];
       split_chunked_route --> upload[Upload workbooks to Circuit, and optimize.];
       upload --> make_manifests[Use **create_manifests_from_circuit** to create the manifests.];
       make_manifests --> print[Review and print the manifests for your drivers.];

Here's the workflow in more detail.

Route all stops with Circuit
----------------------------

When you have all the stops for the day, upload them to Circuit and optimize them in a single route with the title "All HHs". You'll use this single route as a starting point to allocate stops to drivers.

Typically you will download this from Circuit using the website, but you can use :code:`create_manifests_from_circuit` (primarily used later to make the final manifests) to download the routes into a single Excel workbook. (First follow "Activate your env" below.)

.. code:: bash

    create_manifests_from_circuit --start_date 1920-08-18 --all_hhs

The filepath to the *combined workbook* will print to the console; ignore the filepath to the *final manifest*. You will not use it; it's the main output of the tool when you use it later to make the final manifests for the true routes, but the *combined workbook* is a byproduct of that process and the one you need now.

Chunk the stops by driver
-------------------------

From the single optimized route of several hundred stops, you'll next allocate them to each of your roughly 40 drivers. This is a complicated hooman task. You're the hooman. Get a snack or three, make sure you're hydrated, and have a blast. You're gonna love it.

Activate your env
-----------------

Phewf! You have your chunked routes. Now you need to upload it to Circuit and optimize the routes. You're going to split all those routes into their own worksheets grouped in multiple workbooks for several staff to upload to Circuit, and you can use :code:`split_chunked_route` to do that, but first you need to activate the :code:`bfb_delivery` conda environment to use it.

First, open your terminal (e.g., Anaconda Command Prompt), navigate to the correct directory (the one containing your ``config.ini`` file you set up in :doc:`getting_started`), and activate your env:

.. code:: bash

    cd path/to/bfb_delivery/dir
    conda activate my_bfb_delivery_env_name

See :doc:`getting_started` for more information.

Now you're ready to use the command-line tools.

Split the chunked route
-----------------------

Now you split the chunked worksheet into separate spreadsheets ready to upload. Run the :code:`split_chunked_route` to split the chunked worksheet into individual driver worksheets divided into workbooks that staff can upload to Circuit:

.. code:: bash

    split_chunked_route --input_path path/to/chunked_routes.xlsx

The paths to the workbooks will print to the console.

See :doc:`split_chunked_route` for more information, e.g. how to set the number of workbooks. You may also use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    split_chunked_route --help

Upload and optimize routes
--------------------------

Next, each staff member will upload their workbook to Circuit and optimize their routes. See `Circuit documentation <https://help.getcircuit.com/en/collections/1889210-circuit-for-teams/>`_.

Make manifests
--------------

Once everyone has the optimized routes in Circuit, one person can run :code:`create_manifests_from_circuit` to get the routes from Circuit and format them into an Excel workbook ready to print, with headers, aggregate data, and color-coded box types. Pass in the start date of the manifest as "YYYY-MM-DD":

.. code:: bash

    create_manifests_from_circuit --start_date 2021-12-21

The path to the final manifest workbook will print to the console.


See :doc:`create_manifests_from_circuit` for more information, e.g. how to set the output directory. You may also use `--help` to see all the optional arguments in the CLI:

.. code:: bash

    create_manifests_from_circuit --help

.. note::

    This takes about a minute to run as it downloads the routes from Circuit.

Review and print manifests
--------------------------

Finally, review and print the manifests for your drivers.


See Also
--------

:doc:`getting_started`

:doc:`split_chunked_route`

:doc:`create_manifests_from_circuit`

:doc:`CLI`

:doc:`bfb_delivery.api`