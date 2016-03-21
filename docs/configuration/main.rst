Configuration
=============

DistributedLog uses key-value pairs in the `property file format`__ for configuration. These values can be supplied either from a file, jvm system properties, or programmatically.

.. _PropertyFileFormat: http://en.wikipedia.org/wiki/.properties

__ PropertyFileFormat_

In DistributedLog, we only put non-environment related settings in the configuration.
Those environment related settings, such as zookeeper connect string, bookkeeper
ledgers path, should not be loaded from configuration. They should be added in `namespace binding`.

.. toctree::
   :maxdepth: 1

   core
   proxy
   client
   perlog
