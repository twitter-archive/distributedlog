---
layout: default

# Sub-level navigation
sub-nav-group: user-guide
sub-nav-parent: references
sub-nav-pos: 3
sub-nav-title: Available Features
---

.. contents:: Available Features

Features
========

BookKeeper Features
-------------------

*<scope>* is the scope value of the FeatureProvider passed to BookKeeperClient builder. in DistributedLog write proxy, the *<scope>* is 'bkc'.

- *<scope>.repp_disable_durability_enforcement*: Feature to disable durability enforcement on region aware data placement policy. It is a feature that applied for global replicated log only. If the availability value is larger than zero, the region aware data placement policy will *NOT* enfore region-wise durability. That says if a *Log* is writing to region A, B, C with write quorum size *15* and ack quorum size *9*. If the availability value of this feature is zero, it requires *9*
  acknowledges from bookies from at least two regions. If the availability value of this feature is larger than zero, the enforcement is *disabled* and it could acknowledge after receiving *9* acknowledges from whatever regions. By default the availability is zero. Turning on this value to tolerant multiple region failures.

- *<scope>.disable_ensemble_change*: Feature to disable ensemble change on DistributedLog writers. If the availability value of this feature is larger than zero, it would disable ensemble change on writers. It could be used for toleranting zookeeper outage.

- *<scope>.<region>.disallow_bookie_placement*: Feature to disallow choosing a bookie replacement from a given *region* when ensemble changing. It is a feature that applied for global replicated log. If the availability value is larger than zero, the writer (write proxy) will stop choosing a bookie from *<region>* when ensemble changing. It is useful to blackout a region dynamically.

DistributedLog Features
-----------------------

*<scope>* is the scope value of the FeatureProvider passed to DistributedLogNamespace builder. in DistributedLog write proxy, the *<scope>* is 'dl'.

- *<scope>.disable_logsegment_rolling*: Feature to disable log segment rolling. If the availability value is larger than zero, the writer (write proxy) will stop rolling to new log segments and keep writing to current log segments. It is a useful feature to tolerant zookeeper outage.

- *<scope>.disable_write_limit*: Feature to disable write limiting. If the availability value is larger than zero, the writer (write proxy) will disable write limiting. It is used to control write limiting dynamically.

Write Proxy Features
--------------------

- *region_stop_accept_new_stream*: Feature to disable accepting new streams in current region. It is a feature that applied for global replicated log only. If the availability value is larger than zero, the write proxies will stop accepting new streams and throw RegionAvailable exception to client. Hence client will know this region is stopping accepting new streams. Client will be forced to send requests to other regions. It is a feature used for ownership failover between regions.
- *service_rate_limit_disabled*: Feature to disable service rate limiting. If the availability value is larger than zero, the write proxies will disable rate limiting.
- *service_checksum_disabled*: Feature to disable service request checksum validation. If the availability value is larger than zero, the write proxies will disable request checksum validation.
