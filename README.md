Overview
========

**DistributedLog** is the new messaging backend built over [Apache BookKeeper](http://bookkeeper.apache.org/), that provides strict ordering semantics, durability and high availability. Applications don't have to worry about managing the underlying storage infrastructure, replication or writing business logic to maintain strict ordering semantics in their messages.

See: [go/DistributedLog](http://go/DistributedLog) for details.
