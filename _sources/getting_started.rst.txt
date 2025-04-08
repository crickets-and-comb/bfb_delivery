===============
Getting Started
===============

If you're reading this, you are probably an end user who, at the current state of the project, will be using the command-line interface (CLI) to run one or two commands. Before you can use the ``bfb_deliver`` CLI, you'll need to do some initial setup on your machine if it hasn't already been done. This document walks you through that process.

Setting up your machine
-----------------------

You need to install a terminal app, conda, and the ``bfb_delivery`` Python package, and you need to create a couple of files. The files you create should be in the same directory where you'll be running the tool.

First-time setup: Installing the package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``bfb_delivery`` is a Python package. To use it, you should install this package within a virtual environment (a "venv" or an "env"). Here's how.

Install conda and terminal
##########################

A venv is basically a controlled space to install and run stuff. The best way to build an env is to use ``conda``.

Go to https://www.anaconda.com to download and install ``conda`` on your machine. You'll have the option to install Anaconda or Miniconda. Miniconda is a smaller version of Anaconda that doesn't come with all the bells and whistles that Anaconda does. You can install all the packages you need with Miniconda.

But, the full Anaconda installation comes with a terminal, Anaconda Prompt, and you'll need a terminal to work in your env. You can use the Anaconda Prompt that gets installed with Anaconda, or you can use Git Bash or another conda-friendly terminal.

So, if you’re following step-by-step, install the full Anaconda version.

Install Git with Unix tools
###########################

Installing Git with Unix tools isn't strictly necessary, but it makes working in the terminal a lot easier. Go to https://git-scm.com/downloads and download and install Git. When you install Git with the installation wizard, one of the wizard steps will offer the option to install Unix tools. Make sure you check that box. This will install basic tools that the tutorials in this documentation assume you have, like ``ls`` and ``chmod``. There are other ways to install these tools, but Git is easy and reliable.

When installing Git, you'll probably want to stick with the default settings depending on your use case. An exception, in addition to installing Unix tools, may be when you choose your editor. If you're not developing (i.e., using Git to edit the source code), you'll never use this, but if you are, you'll want to choose an editor you like. The default is Vim, which is a powerful editor but has a steep learning curve because of low interface discoverability. If you're not sure, choose Nano or something you are familiar with. I recommend Nano. It's a terminal-based editor that's pretty user-friendly as it has a menu of shortcuts at the bottom of the page.

Build the env and install the package
#####################################

Once you have a terminal and ``conda`` installed, open your terminal. If you installed Anaconda, your terminal will be an app called Anaconda Prompt.

Now, create an env with the following command:

.. code:: bash

    conda create -n bfb_delivery python=3.12 --yes

This will create an env named "bfb_delivery" with Python 3.12 installed. You can name it something else if you'd like, like ``my_bfb_delivery_env_name``.

Activate the env with the following command:

.. code:: bash

    conda activate bfb_delivery

.. note::

    It's important to have the env activated when you install. You want to install the package in the env, not out in the global environment of your machine.

Now install the package in the env:

.. code:: bash

    pip install bfb_delivery

.. note::

    This example happens to name the env the same name as the package you're installing, ``bfb_delivery``. But, this is arbitrary and just for convenience. The package will always be called ``bfb_delivery``, but you can name the env anything. It's worth knowing that the env and the package installed in the env are two distinct things. The env contains the package along with other necessary packages.

First-time setup: File setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned above, this tool relies on a couple of files to run. These files need to be in the same directory as where you're running the tool. The files are:

1. ``config.ini``: This file contains phone numbers that will be used in the final manifests.
2. ``.env``: This file contains your Circuit API key.

You should only need to update these files if the support numbers change or your key expires, preventing the tool from interfacing with Circuit.

You'll probably want to put these files in a special folder where you will always run the tool. You can create a folder for this purpose. For example, you could create a folder called ``bfb_delivery`` in your home directory:

.. code:: bash

    mkdir bfb_delivery

Then, you can navigate to that folder in your terminal with the following command:

.. code:: bash

    cd bfb_delivery

.. note::

    As with the distinction between the package and the env, naming the folder ``bfb_delivery`` is an arbitrary decision for convenience. You can name the folder anything you want. The important thing is that you know where it is and that you put the files in there. The package, the env, and the folder are three distinct things, though they happen to be named the same thing in this example.

Config file setup
#################

You need to create a local config file with phone numbers (which will end up on the top of the final manifests). We store them locally so we don't put phone numbers in the public codebase. The config file should be named ``config.ini`` and should be in the same directory as where you're running the tool, ``bfb_delivery`` if you're following along. The file should look like this:

.. code:: ini

    [phone_numbers]
    driver_support = 555-555-5555
    recipient_support = 555-555-5555 x5

Before creating the file, navigate to the folder you created in the above step. (You should already be in the folder if you followed along step by step.)

If you've been following along step by step, you can use Nano. This will create the file if it doesn't already exist and open it for editing:

.. code:: bash

    nano config.ini

You can add the lines above to the file. To save and exit, press ``CTRL + X``, then ``Y`` to confirm you want to save, and then ``Enter`` to confirm the filename.

.. note::

    Nano does not allow the use of a mouse. You'll use the keyboard only. Use the arrows to move the cursor around. See the menu at the bottom for hotkey commands (to save end exit, e.g.).

To check that the file was created correctly, run the following command:

.. code:: bash

    cat config.ini

.env file setup
###############

You need a Circuit API key to run the tools that interact with Circuit, and it needs to be in a ``.env`` file adjacent to the ``config.ini`` file above.

To get a key, log in to Circuit, click on "Settings" in the sidebar, under "Workspace" in that sidebar click on "Integrations," and under "API" click "Generate New Key." Copy that key and keep it safe, and never share it. The best way to keep it safe is by putting it in a protected file, like a ``.env`` file, which is what you need to do to use the ``bfb_delivery`` tool anyway.

Make sure you don't already have a ``.env`` file:

.. code:: bash

    ls -a

This will list all the files in the current directory, including hidden files (files that start with a dot).

If you don't see a ``.env`` file, create one:

.. code:: bash

    touch .env

Make it secure by setting to read and write only by you:

.. code:: bash

    chmod 600 .env

Open the ``.env`` file in a text editor and add the following line:

.. code:: bash

    CIRCUIT_API_KEY=your_api_key_here

Or, instead of opening the file in a text editor, you can use the following command to add the key:

.. code:: bash

    echo "CIRCUIT_API_KEY=your_api_key_here" >> .env

To check that the key was added correctly, run the following command:

.. code:: bash

    cat .env

Using the package once installed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once the package installed and the local files are set up, to use the tool, you'll need to activate the env and navigate to the directory you setup.

If you're following along, you already have the env activated. But, the next time you need to use the tool, you'll need to make sure the env is activated. To activate your env, open your terminal (if not already open) and tell conda to activate the env with the following command:

.. code:: bash

    conda activate bfb_delivery

Then, navigate to the directory where you set up the files. If you followed along, you can do this with the following command:

.. code:: bash

    cd /example/path/to/bfb_delivery

Phewf, you're ready to use the tool!

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