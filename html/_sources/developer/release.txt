Release
=======

This page is a guide to build release for DistributedLog.

Build package
~~~~~~~~~~~~~

Buidling the packages using `scripts/snapshot`.

::
    
    ./scripts/snapshot


The packages will be generated under `dist/release` directory, including:

- `distributedlog-service-{gitsha}.zip`: This is a binary package to run distributedlog services.
- `distributedlog-benchmark-{gitsha}.zip`: This is a binary package to run distributedlog benchmark.
- `distributedlog-tutorials-{gitsha}.zip`: This is a binary package to run distributedlog tutorials.
- `distributedlog-all-{gitsha}.zip`: This is a binary package contains all the above three packages.
