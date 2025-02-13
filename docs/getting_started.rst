===============
Getting Started
===============

If you're reading this, you are probably an end user who, at the current state of the project, will be using the command-line interface (CLI) to run one or two commands. Before you can use the ``bfb_deliver`` CLI, you'll need to do some initial setup on your machine if it hasn't already been done. This document walks you through that process.

Setting up your machine
-----------------------

You need to create a couple of files and install a terminal app, conda, and the ``bfb_delivery`` Python package. The files you create should be in the same directory where you'll be running the tool.

Config file setup
~~~~~~~~~~~~~~~~~

This tool requires a local config file with phone numbers. We store them locally so we don't put phone numbers in the public codebase. The config file should be named ``config.ini`` and should be in the same directory as where you're running the tool. The file should look like this:

.. code:: ini

    [phone_numbers]
    driver_support = 555-555-5555
    recipient_support = 555-555-5555 x5

.env file setup
~~~~~~~~~~~~~~~

You need a Circuit API key to run the tools that interact with Circuit, and it needs to be in a ``.env`` file adjacent to the ``config.ini`` file above.

To get a key, log in to Circuit, click on "Settings" in the sidebar, under "Workspace" in that sidebar click on "Integrations," and under "API" click "Generate New Key." Copy that key and keep it safe, and never share it. The best way to keep it safe is by putting it in a protected file, like a ``.env`` file, which is what you need to do to use the ``bfb_delivery`` tool anyway.

Make sure you don't already have a ``.env`` file:

.. code:: bash

    ls -a

If you don't see a ``.env`` file, create one:

.. code:: bash

    touch .env

Make it secure by setting to read and write only by you:

.. code:: bash

    chmod 600 .env

Open the ``.env`` file in a text editor and add the following line:

.. code:: bash

    CIRCUIT_API_KEY=your_api_key_here

Or, instead of opening the file in a text editor, you use the following command to add the key:

.. code:: bash

    echo "CIRCUIT_API_KEY=your_api_key_here" >> .env

To check that the key was added correctly, run the following command:

.. code:: bash

    cat .env

First-time setup: Installing the package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``bfb_delivery`` is a Python package. To use it, you should install this package within a virtual environment (a "venv" or an "env"). Here's how.

Install conda and terminal
##########################

A venv is basically a controlled space to install and run stuff. The best way to build an env is to use ``conda``.

Go to https://www.anaconda.com to download and install ``conda`` on your machine. You'll have the option to install Anaconda or Miniconda. Miniconda is a smaller version of Anaconda that doesn't come with all the bells and whistles that Anaconda does. You can install all the packages you need with Miniconda.

But, the full Anaconda installation comes with a terminal, Anaconda Command Prompt, and you'll need a terminal to work in your env. You can use the Anaconda Command Prompt that gets installed with Anaconda, or you can use Git Bash or another conda-friendly terminal.

Build the env and install the package
#####################################

Once you have a terminal and ``conda`` installed, open your terminal and create an env with the following command:

.. code:: bash

    conda create -n bfb_delivery python=3.12 --yes

This will create an env named "bfb_delivery" with Python 3.12 installed. You can name it something else if you'd like.

Activate the env with the following command:

.. code:: bash

    conda activate bfb_delivery

Now install the package in the env:

.. code:: bash

    pip install bfb_delivery

.. note::

    It's important to have the env activated when you install. You want to install the package in the env, not out in the global environment of your machine.

Using the package once installed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once the package installed, all you need to do is activate the env and you're good to go. Open your terminal and activate the env with the following command:

.. code:: bash

    conda activate my_bfb_delivery_env_name

Usage examples
--------------

Here are some examples of how to use this package. See :doc:`further documentation </index>` for your use case.

CLI
~~~

You can use the command-line-interface (CLI) if you have this package installed in your environment. For example:

.. code:: bash

    build_routes_from_chunked --input_path path/to/input.xlsx

See :doc:`CLI` for more information about each tool. Each tool has a `--help` flag to see all the optional arguments in the CLI:

.. code:: bash

    build_routes_from_chunked --help

Library
~~~~~~~

You are likely only going to use the CLI, but here are some guidelines for using the library.

Avoid calling library functions directly and stick to the public API:

.. code:: python

    from bfb_delivery import build_routes_from_chunked
    # These are okay too:
    # from bfb_delivery.api import build_routes_from_chunked
    # from bfb_delivery.api.public import build_routes_from_chunked

    build_routes_from_chunked(input_path="path/to/input.xlsx")

If you're a power user or just want to feel like one, you can use the internal API:

.. code:: python

    from bfb_delivery.api.internal import build_routes_from_chunked

    build_routes_from_chunked(input_path="path/to/input.xlsx")


Nothing is stopping you from importing from :code:`lib` directly, but you should avoid it -- unless you like to tell people, "Danger is my middle name." Here's a taste of danger:

.. code:: python

    from bfb_delivery.lib.dispatch.write_to_circuit import build_routes_from_chunked

    build_routes_from_chunked(input_path="path/to/input.xlsx")

Your workflow
-------------

Once you're set up, and you have a master list of chunked routes you want to split optimize in Circuit, you can begin using this tool. See :doc:`workflow` for how you can use the tools in this package to streamline your delivery route manifest creation process.


See Also
--------

:doc:`workflow`

:doc:`developers`