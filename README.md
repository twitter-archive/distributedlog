[![Build Status](https://travis-ci.org/apache/incubator-distributedlog.svg?branch=master)](https://travis-ci.org/apache/incubator-distributedlog)

## Overview

DistributedLog (DL) is a high-performance, replicated log service, offering
durability, replication and strong consistency as essentials for building
reliable distributed systems.

#### High Performance

DL is able to provide milliseconds latency on durable writes with a large number
of concurrent logs, and handle high volume reads and writes per second from
thousands of clients.

#### Durable and Consistent

Messages are persisted on disk and replicated to store multiple copies to
prevent data loss. They are guaranteed to be consistent among writers and
readers in terms of strict ordering.

#### Efficient Fan-in and Fan-out

DL provides an efficient service layer that is optimized for running in a multi-
tenant datacenter environment such as Mesos or Yarn. The service layer is able
to support large scale writes (fan-in) and reads (fan-out).

#### Various Workloads

DL supports various workloads from latency-sensitive online transaction
processing (OLTP) applications (e.g. WAL for distributed database and in-memory
replicated state machines), real-time stream ingestion and computing, to
analytical processing.

#### Multi Tenant

To support a large number of logs for multi-tenants, DL is designed for I/O
isolation in real-world workloads.

#### Layered Architecture

DL has a modern layered architecture design, which separates the stateless
service tier from the stateful storage tier. To support large scale writes (fan-
in) and reads (fan-out), DL allows scaling storage independent of scaling CPU
and memory.

## Documentation and Getting Started

* [**Getting Started**](http://twitter.github.io/distributedlog/html/basics/quickstart.html)
* [**API References**](http://twitter.github.io/distributedlog/html/api/main.html)
* [**Tutorials**](http://twitter.github.io/distributedlog/html/tutorials/main.html)

## Getting involved

* Website: https://twitter.github.io/distributedlog/html/
* Source: https://github.com/twitter/distributedlog
* Maiing List: [distributedlog-user@googlegroups.com](https://groups.google.com/forum/#!forum/distributedlog-user)
* Issue Tracker: https://github.com/twitter/distributedlog/issues

We feel that a welcoming community is important and we ask that you follow Twitter's
[Open Source Code of Conduct](https://engineering.twitter.com/opensource/code-of-conduct)
in all interactions with the community.

## Authors

* Robin Dhamankar ([@RobinDhamankar](https://twitter.com/RobinDhamankar))
* Sijie Guo ([@sijieg](https://twitter.com/sijieg))
* Leigh Stewart ([@l4stewar](https://twitter.com/l4stewar))

Thanks for assistance and contributions:

* Aniruddha Laud ([@i0exception](https://twitter.com/i0exception))
* Franck Cuny ([@franckcuny](https://twitter.com/franckcuny))
* Dave Rusek ([@davidjrusek](https://twitter.com/davidjrusek))
* Jordan Bull ([@jordangbull](https://twitter.com/jordangbull))

A full list of [contributors](https://github.com/twitter/distributedlog/graphs/contributors) can be found on GitHub.

## License
Copyright 2016 Twitter, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
