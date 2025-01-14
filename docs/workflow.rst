==============================
The Delivery-Planning Workflow
==============================

Once you're set up (see :doc:`getting_started`), and you have a master list of chunked routes you want to split up and optimize in Circuit, you can begin using this tool. Here's the workflow.

.. mermaid::
   :caption: Delivery Planning Flowchart

   graph TD;
       get_long_route[Route all stops with Circuit.] --> chunk["Chunk" stops by driver.];
       chunk --> activate_env[Activate your env.];
       activate_env --> split_chunked_route[Use **bfb_delivery.split_chunked_route** to create the workbook for upload to Circuit.];
       split_chunked_route --> upload[Upload workbooks to Circuit, and download the optimized route CSVs.];
       upload --> move_CSV[Move all the CSVs to a single directory.];
       move_CSV --> make_manifests[Use **bfb_delivery.create_manifests** to create the manifests.];
       make_manifests --> print[Review and print the manifests for your drivers.];

Activate your env
-----------------

First, open your terminal (e.g., Anaconda Command Prompt), navigate to the correct directory (the one containing your ``config.ini`` file defined above), and activate your env:

.. code:: bash

    cd path/to/bfb_delivery/dir
    conda activate my_bfb_delivery_env_name

See :doc:`getting_started` for more information.

Split the chunked route
-----------------------

Then, run the :code:`split_chunked_route` to split the chunked worksheet into individual driver worksheets divided into workbooks that staff can upload to Circuit:

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx

The paths to the workbooks will print to the console.

See :doc:`split_chunked_route` for more information, e.g. how to set the number of workbooks.  Use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    split_chunked_route --help

Upload and optimize routes
--------------------------

Next, upload the workbooks to Circuit and optimize the routes. If you split the routes among staff to upload and download separately, you'll need to move them all to a single directory.

You can do this however you'd like, but one way is to use the command line:

.. code:: bash

    mv path/to/downloaded_1/*.csv path/to/single_directory/
    mv path/to/downloaded_2/*.csv path/to/single_directory/
    ...

Make manifests
--------------

Once you have the optimized routes saved as CSVs in a single directory (without other CSVs in it), run :code:`create_manifests` to combine the driver workbooks into a single workbook ready to print:

.. code:: bash

    create_manifests --input_dir path/to/input/

The path to the combined and formatted workbook will print to the console.

.. note::

    You should only put the CSVs you want to include in the manifest in the directory. The tool will combine all CSVs in the directory into a single workbook.

See :doc:`create_manifests` for more information, e.g. how to set the date used in the manifest. Use `--help` to see all the optional arguments in the CLI.

.. code:: bash

    create_manifests --help

Review and print manifests
--------------------------

Finally, review and print the manifests for your drivers.


See Also
--------

:doc:`getting_started`

:doc:`split_chunked_route`

:doc:`create_manifests`

:doc:`CLI`

:doc:`bfb_delivery.api`