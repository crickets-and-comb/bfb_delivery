===========================================
Reference Package: A basic package template
===========================================

Includes typical CLI and library setup. To include service app setup at some point.

Contents
--------

.. toctree::
   :maxdepth: 3

   reference_package


Library
-------

Avoid calling library functions directly and stick to the public API:

.. code:: python

    from reference_package import wait_a_second

    wait_a_second()

If you're a power user, you can use the internal API:

.. code:: python

    from reference_package.api.internal import wait_a_second

    wait_a_second()


Nothing is stopping you from importing from lib directly, but you should avoid it unless you're developing:

.. code:: python

    from reference_package.lib.example import wait_a_second

    wait_a_second()