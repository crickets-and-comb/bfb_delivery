==============================
Intro to Command Line and Bash
==============================

If you are going to use the ``bfb_delivery`` command-line interface (CLI), you will need to know how to use the command line and Bash in a couple of very simple ways. This document gives you an introduction to using the command line and Bash, specifically the builtin tools and commands you will use to navigate your file system. It also gives you extra tutorials that you don't necessarily need to use the CLI, but that you may find useful.

To use the ``bfb_delivery`` CLI, you will at least need to know how to understand the basic command prompt and how to navigate your file system. This is covered in the first two sections, :ref:`terminal` and :ref:`navigation`, but there are additional sections on manipulating files and directories, reading files, writing to files, and using wildcards.

.. note::

    This document is not a comprehensive guide to the command line or Bash. It is a brief introduction to the tools you will use in the ``bfb_delivery`` CLI. If you want to learn more about the command line and Bash, there are many resources available online.

.. _terminal:

The "command prompt" or "terminal"
----------------------------------

There are a number of applications you can download to interact with the command line. On Windows, you can use Command Prompt or PowerShell. On macOS, you can use Terminal. On Linux, you can use the terminal that comes with your distribution. You can also download other terminal applications like Git Bash or Anaconda Command Prompt.

For our purposes, I recommend using Anaconda Command Prompt, since it comes with conda and Unix-style commands and is Windows compatible. You can download Anaconda at https://www.anaconda.com.

When you open the terminal, you will see a prompt that looks something like this:

.. code:: bash

    (base) MacBook-Pro:bfb_delivery me$ 

The first part of the prompt in parens is the conda environment you have activated. It may also be absent if you have no env activated. The second part of the prompt is the name of the computer, followed by a colon and the current directory you are in. Then you see your username followed by a dollar sign. The dollar sign indicates that the terminal is ready to accept commands. So, in this example we have the base env activated on a MacBook-Pro, we are in the ``bfb_delivery`` directory, and our username is "me".

.. _navigation:

Navigating your file system
---------------------------

The first thing you need to know how to do is navigate your file system. Here are some basic commands you will use to do that:

- ``pwd``: Print working directory. This command will show you the full path to the directory you are currently in.
- ``ls``: List files. This command will show you a list of files and directories in the current directory.
- ``cd``: Change directory. This command will allow you to move to a different directory.

.. note::

    Filepaths can be either absolute or relative. An absolute path starts from the root directory, while a relative path starts from the current directory. For instance, if you are in the directory ``/Users/me/Documents/bfb_delivery``, the relative path to a file named ``file.txt`` within that directory would be ``file.txt`` and the absolute path would be ``/Users/me/Documents/bfb_delivery/file.txt``.

.. attention::

    The root directory is the top-level directory in a file system. On Windows, it is usually ``C:\``. On macOS and Linux, it is simply ``/``. Also, Windows uses backslashes (``\``) in filepaths, while macOS and Linux use forward slashes (``/``).

Present working directory
~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``pwd`` command to see the full path to the directory you are currently in. Here is an example:

.. code:: bash

    $ pwd
    /Users/me/Documents/bfb_delivery

.. note::

    Notice I left the dollar sign off the prompt in the example. You don't need to type the dollar sign when you are typing commands in the terminal. This is just one way to show you that you are typing commands in the terminal. Often other examples exclude the dollar sign altogether.

Listing files and directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use options with the ``ls`` command to get more information about the files and directories in the current directory. Here are some common options:

- ``-l``: Long format. This option will show you more information about the files and directories, including permissions, owner, group, size, and modification date.
- ``-a``: All files. This option will show you all files and directories, including hidden files and directories (those that start with a dot, like ``.env``).
- ``-h``: Human-readable. This option will show you file sizes in a human-readable format, like kilobytes, megabytes, etc.
- ``-t``: Sort by modification time. This option will sort the files and directories by the time they were last modified.

I tend to to use ``ls -lha`` to get a long format list of all files, including hidden files, in a human-readable format. In addition to all the extra info that I like, it lists them vertically instead of horizontally, which I find easier to read:

.. code:: bash

    (base) My-MacBook-Pro:bfb_delivery me$ ls -alh
    total 88
    drwxr-xr-x@ 20 me  staff   640B Feb 12 19:33 .
    drwxr-xr-x@ 12 me  staff   384B Feb  2 19:58 ..
    -rw-r--r--@  1 me  staff    89B Feb 12 14:46 .env
    drwxr-xr-x@ 16 me  staff   512B Feb 12 21:45 .git
    drwxr-xr-x@  5 me  staff   160B Dec 22 19:05 .github
    -rw-r--r--@  1 me  staff   746B Dec 27 08:42 .gitignore
    -rw-r--r--@  1 me  staff   102B Dec 23 23:31 .gitmodules
    drwx------@ 11 me  staff   352B Feb  1 17:50 .test_data
    -rw-r--r--@  1 me  staff   1.0K Dec 22 19:05 LICENSE
    -rw-r--r--@  1 me  staff   347B Jan  3 22:37 Makefile
    -rw-r--r--@  1 me  staff   8.7K Feb 12 21:17 README.md
    -rw-r--r--@  1 me  staff    81B Jan  2 09:06 config.ini
    drwxr-xr-x@  4 me  staff   128B Feb 12 19:19 dist
    drwxr-xr-x@ 26 me  staff   832B Feb 12 21:46 docs
    -rw-r--r--@  1 me  staff    82B Dec 22 19:05 pyproject.toml
    drwxr-xr-x@  8 me  staff   256B Feb 12 21:44 scripts
    -rw-r--r--@  1 me  staff   1.9K Feb 12 13:01 setup.cfg
    drwxr-xr-x@ 15 me  staff   480B Feb 12 12:13 shared
    drwxr-xr-x@  4 me  staff   128B Dec  6 17:01 src
    drwxr-xr-x@  7 me  staff   224B Feb 12 21:44 tests
    (base) My-MacBook-Pro:bfb_delivery me$ 

The first part of each line is the permissions, the second part is the number of links to the file or directory, the third part is the owner, the fourth part is the group, the fifth part is the size, the sixth part is the modification date, and the last part is the name of the file or directory. You can tell if an item is a file or a directory by the first character in the permissions; if it is a ``d``, it is a directory, if it is a ``-``, it is a file.

There are three types of permissions: read (``r``), write (``w``), and execute (``x``). They come in three groups: owner, group, and others. The owner is the user who owns the file or directory, the group is the group that owns the file or directory, and others are everyone else. For instance, the ``scripts`` directory is set to allow the owner, group, and everyone else to read and execute, but only the owner can write to it.

You can pass any filepath to ``ls`` to list the files and directories in that directory. For instance, you can use ``ls /path/to/directory`` to list the files and directories in a different directory.

.. tip::

    You can use the tab key to autocomplete file and directory names. This is especially useful when you are typing long file and directory names.

tree
^^^^

If you have the ``tree`` command installed, you can use it to see a tree view of your file system. This is especially useful when you have a lot of files and directories. Here is an example:

.. code:: bash

    $ tree
    .
    ├── LICENSE
    ├── Makefile
    ├── README.md
    ├── config.ini
    ├── dist
    │   ├── bfb_delivery-0.6.0-py3-none-any.whl
    │   └── bfb_delivery-0.6.0.tar.gz
    ├── docs
    │   ├── CLI.rst
    │   ├── _build
    │   │   ├── CLI.html
    │   │   ├── _sources
    ...

This command will show you a tree view of your file system.

You can see additional options for the ``tree`` command by typing ``man tree`` in the terminal, or by typing ``tree --help``.

.. tip::

    Many tools include a ``--help`` option that will show you the options available for that tool. Many tools also include a manual page that you can access with the ``man`` command. For instance, there is no ``--help`` option for the ``ls`` command, but you can type ``man ls`` to see the manual page for the ``ls`` command.

Changing directories
~~~~~~~~~~~~~~~~~~~~

You can use the ``cd`` command to change directories. Here are some examples:

- ``cd path/to/directory``: Change to a directory that is a subdirectory of the current directory. This is also known as a relative path.
- ``cd /path/to/directory``: Change to a directory that is an absolute path. This means you are starting from the root directory.
- ``cd ..``: Change to the parent directory of the current directory.
- ``cd ~``: Change to your home directory. Simply typing ``cd`` will also take you to your home directory.

.. note::

    Notice how the parent directory is represented by two dots. Look at the previous section to see how the parent directory is represented in the output of the ``ls -lha`` command. It also shows you the parent directory as two dots. A single dot represents the current directory.

Manipulating files and directories
----------------------------------

You can use a number of commands to manipulate files and directories. Here are some basic commands you may use:

- ``mkdir``: Make directory. This command will create a new directory.
- ``touch``: Create file. This command will create a new file.
- ``cp``: Copy. This command will copy a file or directory.
- ``mv``: Move. This command will move a file or directory. You can also use it to rename a file or directory, by basically moving it to the same location with a different name.
- ``rm``: Remove. This command will delete a file or directory. Be careful with this command, as it will not ask you to confirm the deletion.

Making a directory
~~~~~~~~~~~~~~~~~~

You can use the ``mkdir`` command to create a new directory. Here is an example:

.. code:: bash

    $ mkdir new_directory

This command will create a new directory called ``new_directory`` in the current directory. You can use an absolute path to create a directory in a different location:

.. code:: bash

    $ mkdir /path/to/new_directory

Creating a file
~~~~~~~~~~~~~~~

You can use the ``touch`` command to create a new file. Here is an example:

.. code:: bash

    $ touch new_file.txt

This command will create a new file called ``new_file.txt`` in the current directory.

Copying files and directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``cp`` command to copy a file. Here is an example:

.. code:: bash

    $ cp file.txt copy_of_file.txt

This command will create a copy of ``file.txt`` called ``copy_of_file.txt`` in the current directory.

To copy a whole directory, you can use the ``-r`` option:

.. code:: bash

    $ cp -r directory copy_of_directory

Moving files and directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``mv`` command to move a file. Here is an example:

.. code:: bash

    $ mv file.txt new_location/file.txt

This command will move ``file.txt`` to the directory ``new_location``.

To rename a file, you can move it to the same location with a different name:

.. code:: bash

    $ mv file.txt new_file.txt

This command will rename ``file.txt`` to ``new_file.txt``.

To mv a whole directory, you can use the ``-r`` option:

.. code:: bash

    $ mv -r directory new_location/directory

Removing files and directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``rm`` command to remove a file. Here is an example:

.. code:: bash

    $ rm file.txt

This command will delete ``file.txt`` from the current directory.

To remove a whole directory, you can use the ``-r`` option:

.. code:: bash

    $ rm -r directory

This command will delete the directory ``directory`` and all its contents.

Wildcards
---------

You can use wildcards to match multiple files or directories. The most common wildcard is the asterisk (``*``), which matches any number of characters. Here are some examples:

.. code:: bash

    $ ls *.txt

This command will list all files that end in ``.txt``.

.. code:: bash

    $ mv *.txt text_files/

This command will move all files that end in ``.txt`` to the directory ``text_files``.


Reading files
-------------

You can use a number of commands to read files. Here are some basic commands you may use:

- ``cat``: Concatenate. This command will display the contents of a file.
- ``less``: This command will display the contents of a file one page at a time.
- ``head``: This command will display the first few lines of a file.
- ``tail``: This command will display the last few lines of a file.
- ``grep``: This command will search for a string in a file.

Displaying the contents of a file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``cat`` command to display the contents of a file. Here is an example:

.. code:: bash

    $ cat file.txt

This command will display the contents of ``file.txt`` in the terminal.


Writing to files
----------------

You can use a number of commands to write to files. Here are some basic commands you may use:

- ``echo``: This command will display a string in the terminal.
- ``>>``: This command will append a string to a file.
- ``>``: This command will overwrite a file with a string.

You can also use graphical text editors like Notepad or TextEdit to write to files, but we will focus on the command line here. There are some text editors you can use in the terminal, like ``nano`` or ``vim``.

Displaying a string in the terminal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``echo`` command to display a string in the terminal. Here is an example:

.. code:: bash

    $ echo "Hello, world!"

This command will display ``Hello, world!`` in the terminal.

Appending a string to a file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``>>`` command to append a string to a file. Here is an example:

.. code:: bash

    $ echo "Hello, world!" >> file.txt

This command will append ``Hello, world!`` to ``file.txt``. (You can use ``cat`` to see the contents of ``file.txt``.)

Overwriting a file with a string
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the ``>`` command to overwrite a file with a string. Here is an example:

.. code:: bash

    $ echo "Hello, world!" > file.txt

Using nano
~~~~~~~~~~

You can use the ``nano`` text editor to write to files. Here is an example:

.. code:: bash

    $ nano file.txt

This command will open the ``file.txt`` in the ``nano`` text editor. If the file doesn't exist, it will create it, and if it does exist you will see the contents and find your cursor at the top. You can write to the file, save it, and exit the editor. To save and exit, press ``Ctrl`` + ``O`` to save, then press ``Enter`` to confirm the filename, then press ``Ctrl`` + ``X`` to exit. There are additional commands at the bottom of the ``nano`` editor window when it is open.