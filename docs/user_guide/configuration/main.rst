---
layout: default

# Top navigation
top-nav-group: user-guide
top-nav-pos: 5
top-nav-title: Configuration

# Sub-level navigation
sub-nav-group: user-guide
sub-nav-parent: user-guide
sub-nav-id: configuration
sub-nav-pos: 5
sub-nav-title: Configuration
---

Configuration
=============

DistributedLog uses key-value pairs in the `property file format`__ for configuration. These values can be supplied either from a file, jvm system properties, or programmatically.

.. _PropertyFileFormat: http://en.wikipedia.org/wiki/.properties

__ PropertyFileFormat_

In DistributedLog, we only put non-environment related settings in the configuration.
Those environment related settings, such as zookeeper connect string, bookkeeper
ledgers path, should not be loaded from configuration. They should be added in `namespace binding`.

- `Core Library Configuration`_

.. _Core Library Configuration: ./core

- `Write Proxy Configuration`_

.. _Write Proxy Configuration: ./proxy

- `Write Proxy Client Configuration`_

.. _Write Proxy Client Configuration: ./client

- `Per Stream Configuration`_

.. _Per Stream Configuration: ./perlog
