---
layout: default

# Top navigation
top-nav-group: admin-guide
top-nav-pos: 1
top-nav-title: Operations

# Sub-level navigation
sub-nav-group: admin-guide
sub-nav-parent: admin-guide
sub-nav-id: operations
sub-nav-pos: 1
sub-nav-title: Operations
---

.. contents:: DistributedLog Operations

DistributedLog Operations
=========================

Feature Provider
~~~~~~~~~~~~~~~~

DistributedLog uses a `feature-provider` library provided by Apache BookKeeper for managing features
dynamically at runtime. It is a feature-flag_ system used to proportionally control what features
are enabled for the system. In other words, it is a way of altering the control in a system without
restarting it. It can be used during all stages of development, its most visible use case is on
production. For instance, during a production release, you can enable or disable individual features,
control the data flow through the system, thereby minimizing risk of system failure in real time.

.. _feature-flag: https://en.wikipedia.org/wiki/Feature_toggle

This `feature-provider` interface is pluggable and easy to integrate with any configuration management
system.

API
___

`FeatureProvider` is a provider that manages features under different scopes. The provider is responsible
for loading features dynamically at runtime. A `Feature` is a numeric flag that control how much percentage
of this feature will be available to the system - the number is called `availability`.

::

    Feature.name() => returns the name of this feature
    Feature.availability() => returns the availability of this feature
    Feature.isAvailable() => returns true if its availability is larger than 0; otherwise false


It is easy to obtain a feature from the provider by just providing a feature name.

::

    FeatureProvider provider = ...;
    Feature feature = provider.getFeature("feature1"); // returns the feature named 'feature1'

    
The `FeatureProvider` is scopable to allow creating features in a hierarchical way. For example, if a system
is comprised of two subsystems, one is *cache*, while the other one is *storage*. so the features belong to
different subsystems can be created under different scopes.

::

    FeatureProvider provider = ...;
    FeatureProvider cacheFeatureProvider = provider.scope("cache");
    FeatureProvider storageFeatureProvider = provider.scope("storage");
    Feature writeThroughFeature = cacheFeatureProvider.getFeature("write_through");
    Feature duralWriteFeature = storageFeatureProvider.getFeature("dural_write");

    // so the available features under `provider` are: (assume scopes are separated by '.')
    // - 'cache.write_through'
    // - 'storage.dural_write'


The feature provider could be passed to `DistributedLogNamespaceBuilder` when building the namespace,
thereby it would be used for controlling the features exposed under `DistributedLogNamespace`.

::

    FeatureProvider rootProvider = ...;
    FeatureProvider dlFeatureProvider = rootProvider.scope("dlog");
    DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
        .uri(uri)
        .conf(conf)
        .featureProvider(dlFeatureProvider)
        .build();


The feature provider is loaded by reflection on distributedlog write proxy server. You could specify
the feature provider class name as below. Otherwise it would use `DefaultFeatureProvider`, which disables
all the features by default.

::

    featureProviderClass=com.twitter.distributedlog.feature.DynamicConfigurationFeatureProvider



Configuration Based Feature Provider
____________________________________

Beside `DefaultFeatureProvider`, distributedlog also provides a file-based feature provider - it loads
the features from properties files.

All the features and their availabilities are configured in properties file format. For example,

::

    cache.write_through=100
    storage.dural_write=0


You could configure `featureProviderClass` in distributedlog configuration file by setting it to
`com.twitter.distributedlog.feature.DynamicConfigurationFeatureProvider` to enable file-based feature
provider. The feature provider will load the features from two files, one is base config file configured
by `fileFeatureProviderBaseConfigPath`, while the other one is overlay config file configured by
`fileFeatureProviderOverlayConfigPath`. Current implementation doesn't differentiate these two files
too much other than the `overlay` config will override the settings in `base` config. It is recommended
to have a base config file for storing the default availability values for your system and dynamically
adjust the availability values in overlay config file.

::

    featureProviderClass=com.twitter.distributedlog.feature.DynamicConfigurationFeatureProvider
    fileFeatureProviderBaseConfigPath=/path/to/base/config
    fileFeatureProviderOverlayConfigPath=/path/to/overlay/config
    // how frequent we reload the config files
    dynamicConfigReloadIntervalSec=60


Available Features
__________________

Check the Features_ reference page for the features exposed by DistributedLog.

.. _Features: ../user_guide/references/features

`dlog`
~~~~~~

A CLI is provided for inspecting DistributedLog streams and metadata.

.. code:: bash

   dlog
   JMX enabled by default
   Usage: dlog <command>
   where command is one of:
       local               Run distributedlog sandbox
       example             Run distributedlog example
       tool                Run distributedlog tool
       proxy_tool          Run distributedlog proxy tool to interact with proxies
       balancer            Run distributedlog balancer
       admin               Run distributedlog admin tool
       help                This help message

   or command is the full name of a class with a defined main() method.

   Environment variables:
       DLOG_LOG_CONF        Log4j configuration file (default $HOME/src/distributedlog/distributedlog-service/conf/log4j.properties)
       DLOG_EXTRA_OPTS      Extra options to be passed to the jvm
       DLOG_EXTRA_CLASSPATH Add extra paths to the dlog classpath

These variable can also be set in conf/dlogenv.sh

Create a stream
_______________

To create a stream:

.. code:: bash

   dlog tool create -u <DL URI> -r <STREAM PREFIX> -e <STREAM EXPRESSION>


List the streams
________________

To list all the streams under a given DistributedLog namespace:

.. code:: bash

   dlog tool list -u <DL URI>

Show stream's information
_________________________

To view the metadata associated with a stream:

.. code:: bash

   dlog tool show -u <DL URI> -s <STREAM NAME>


Dump a stream
_____________

To dump the items inside a stream:

.. code:: bash

   dlog tool dump -u <DL URI> -s <STREAM NAME> -o <START TXN ID> -l <NUM RECORDS>

Delete a stream
_______________

To delete a stream, run:

.. code:: bash

   dlog tool delete -u <DL URI> -s <STREAM NAME>


Truncate a stream
_________________

Truncate the streams under a given DistributedLog namespace. You could specify a filter to match the streams that you want to truncate.

There is a difference between the ``truncate`` and ``delete`` command. When you issue a ``truncate``, the data will be purge without removing the streams. A ``delete`` will delete the stream. You can pass the flag ``-delete`` to the ``truncate`` command to also delete the streams.

.. code:: bash

   dlog tool truncate -u <DL URI>
