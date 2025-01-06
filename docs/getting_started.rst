===============
Getting Started
===============

If you're reading this, you are probably an end user who, at the current state of the project, will be using the command-line interface (CLI) to run one or two commands. Before you can use the CLI, you'll need to do some initial setup on your machine if it hasn't already been done. This document will walk you through that process.

Setting up your machine
-----------------------

Config file setup
^^^^^^^^^^^^^^^^^^

This tool requires a local config file with phone numbers. We store them locally so we don't put phone numbers in the codebase. The config file should be named ``config.ini`` and should be in the same directory as where you're running the tool. The file should look like this:

.. code:: ini

    [phone_numbers]
    driver_support = 555-555-5555
    recipient_support = 555-555-5555 x5

First-time setup: Installing the package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``bfb_delivery`` is a Python package. To use it, you should install this package within a virtual environment (a "venv" or an "env"). Here's how.

Install conda and terminal
##########################

A venv is basically a controlled space to install and run stuff. The best way to build an env is to use ``conda``.

Go to https://www.anaconda.com to download and install ``conda`` on your machine. You'll have the option to install Anaconda or Miniconda. Miniconda is a smaller version of Anaconda that doesn't come with all the bells and whistles that Anaconda does. You can install all the packages you need with Miniconda.

But, you'll also need a terminal to work in your env. You can use the Anaconda Prompt that gets installed with Anaconda (or Git Bash, or another conda-friendly terminal).

Build the env and install the package
#####################################

Once you have a terminal and ``conda`` installed, open your terminal and create an env with the following command:

.. code:: bash

    conda create -n bfb_delivery_py3.12 python=3.12 --yes

This will create an env named "bfb_delivery_py3.12" with Python 3.12 installed. You can name it something else if you'd like.

Activate the env with the following command:

.. code:: bash

    conda activate bfb_delivery_py3.12

Now install the package in the env:

.. code:: bash

    pip install bfb_delivery

.. note::

    It's important to have the env activated when you install. You want to install the package in the env, not out in the global environment of your machine.

Using the package if it's already installed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the package is already installed, all you need to do is activate the env and you're good to go. Open your terminal and activate the env with the following command:

.. code:: bash

    conda activate my_bfb_delivery_env_name

Usage examples
--------------

Here are some examples of how to use this package. See :doc:`further documentation </index>` for your use case.

CLI
^^^

You can use the command-line-interface (CLI) if you have this package installed in your environment. For example:

.. code:: bash

    split_chunked_route --input_path path/to/input.xlsx

See :doc:`CLI` for more information.

Library
^^^^^^^

You are likely only going to use the CLI, but here are some guidelines for using the library.

Avoid calling library functions directly and stick to the public API:

.. code:: python

    from bfb_delivery import split_chunked_route
    # These are okay too:
    # from bfb_delivery.api import split_chunked_route
    # from bfb_delivery.api.public import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")

If you're a power user or just want to feel like one, you can use the internal API:

.. code:: python

    from bfb_delivery.api.internal import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")


Nothing is stopping you from importing from :code:`lib` directly, but you should avoid it -- unless you like to tell people, "Danger is my middle name." Here's a taste of danger:

.. code:: python

    from bfb_delivery.lib.formatting.sheet_shaping import split_chunked_route

    split_chunked_route(input_path="path/to/input.xlsx")

Your workflow
-------------

Once you're set up, and you have a master list of chunked routes you want to split up and optimize in Circuit, you can begin using this tool. See :doc:`workflow` for how you can use the tools in this package to streamline your delivery route manifest creation process.