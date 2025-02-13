===
CLI
===

Each tool includes a `--help` flag to see all the optional arguments in the CLI. For example:

.. code:: bash

    build_routes_from_chunked --help

.. click:: bfb_delivery.cli.build_routes_from_chunked:main
   :prog: build_routes_from_chunked
   :nested: full

.. click:: bfb_delivery.cli.split_chunked_route:main
   :prog: split_chunked_route
   :nested: full

.. click:: bfb_delivery.cli.create_manifests_from_circuit:main
   :prog: create_manifests_from_circuit
   :nested: full

.. click:: bfb_delivery.cli.create_manifests:main
   :prog: create_manifests
   :nested: full

. click:: bfb_delivery.cli.combine_route_tables:main
   :prog: combine_route_tables
   :nested: full

.. click:: bfb_delivery.cli.format_combined_routes:main
   :prog: format_combined_routes
   :nested: full


See Also
--------

:doc:`bfb_delivery.cli`

:doc:`bfb_delivery.api`