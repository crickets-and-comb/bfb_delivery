===================
Developer Resources
===================

This project is based on the Crickets and Comb ``reference_package`` template. For more information on setting up and using dev tools, `reference_package repo <https://github.com/crickets-and-comb/reference_package/>`_ and the `shared repo <https://github.com/crickets-and-comb/shared/>`_.

See also the `BFB Delivery GitHub repository <https://github.com/crickets-and-comb/bfb_delivery/>`_.

If you've already cloned the repo and set up the ``shared`` Git submodule, check your make targets by running:

.. code:: bash

    make

Circuit API
-----------

The Circuit API is used to create, optimize, and retrieve routes etc.. The API is documented in the `Circuit API documentation <https://developer.team.getcircuit.com/api>`_.

Data structure
^^^^^^^^^^^^^^

At a high level, the data structure is:

.. mermaid::

    graph TD;
        A[Plan] -->|m:m| B[Drivers]
        A -->|1:m| C[Stops]
        B -->|1:m| D[Routes]
        D -->|1:m| C[Stops]

So, in Circuit, a plan can have multiple drivers, and routes only get created when a plan with stops and drivers is created and optimized. Optimization allocates stops to the drivers in the plan, with one route per driver in the plan. A driver may have multiple routes, and plan may have multiple routes.

But, for the Bellingham Food Bank, there is only ever one route per plan. They only assign a single driver to each plan. A driver may still have multiple routes. If a driver has multiple routes, they number the plan titles (e.g., "1.17 Tim #1", "1.17 Tim #2"). Plan titles function essentially as route IDs, and plan titles are also a quasi-ID for drivers. The ``plan:driver`` relationship goes from ``m:m`` in Circuit to ``m:1`` for the food bank.

.. mermaid::

    graph TD;
        A[Plan] -->|**m:1**| B[Drivers]
        A -->|1:m| C[Stops]
        B -->|1:m| D[Routes]
        D -->|1:m| C[Stops]


Additionally, the data come from the API in a JSON structure that is not normalized. So, routes docs do contain plan IDs, and plan docs contain route IDs. And, the food bank is not ready to maintain an RDBMS for this purpose.

Given the data structure and format, :py:func:`bfb_delivery.api.public.create_manifests_from_circuit` has built-in validations to ensure normalized relationships within the stricter special case of the food bank's use case. See :mod:`bfb_delivery.lib.schema`.


See Also
--------

:doc:`getting_started`

:doc:`workflow`

:doc:`CLI`

:doc:`bfb_delivery.api`

`BFB Delivery GitHub repository <https://github.com/crickets-and-comb/bfb_delivery/>`_

`bfb-delivery PyPi distribution <https://pypi.org/project/bfb-delivery/>`_

`reference_package repo <https://github.com/crickets-and-comb/reference_package/>`_