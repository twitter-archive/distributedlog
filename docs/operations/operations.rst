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

Check the :doc:`../references/features` reference page for the features exposed by DistributedLog.
