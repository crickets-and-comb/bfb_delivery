===========================================
Reference Package: A basic package template
===========================================

Includes typical CLI and library setup. To include service app setup at some point.

Contents
--------

.. toctree::
   :maxdepth: 3

   bfb_delivery


Library
-------

Avoid calling library functions directly and stick to the public API:

.. code:: python

    from bfb_delivery import wait_a_second

    wait_a_second()

If you're a power user, you can use the internal API:

.. code:: python

    from bfb_delivery.api.internal import wait_a_second

    wait_a_second()


Nothing is stopping you from importing from lib directly, but you should avoid it unless you're developing:

.. code:: python

    from bfb_delivery.lib.example import wait_a_second

    wait_a_second()