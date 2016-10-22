![logo](/website/images/distributedlog_logo_m.png?raw=true "Apache DistributedLog logo")

[![Build Status](https://travis-ci.org/apache/incubator-distributedlog.svg?branch=master)](https://travis-ci.org/apache/incubator-distributedlog)
[![Build Status](https://builds.apache.org/buildStatus/icon?job=distributedlog-nightly-build)](https://builds.apache.org/job/distributedlog-nightly-build/)
[![Coverage Status](https://coveralls.io/repos/github/apache/incubator-distributedlog/badge.svg?branch=master)](https://coveralls.io/github/apache/incubator-distributedlog?branch=master)

# Apache DistributedLog (incubating)

Apache DistributedLog (DL) is a *high-throughput*, *low-latency* replicated log service, offering
*durability*, *replication* and *strong consistency* as essentials for building
reliable _real-time_ applications.

## Status

_The Apache DistributedLog project is in the process of incubating. This includes the creation of project resources,
the refactoring of the initial code submissions, and the formulation of project documentation, planning and the
improvements of existing user and operation documents. Any feedback and contributions are welcome._

## Features

#### High Performance

DL is able to provide *milliseconds* latency on *durable* writes with a large number
of concurrent logs, and handle high volume reads and writes per second from
thousands of clients.

#### Durable and Consistent

Messages are persisted on disk and replicated to store multiple copies to
prevent data loss. They are guaranteed to be consistent among writers and
readers in terms of *strict ordering*.

#### Efficient Fan-in and Fan-out

DL provides an efficient service layer that is optimized for running in a multi-
tenant datacenter environment such as _Mesos_ or _Yarn_. The service layer is able
to support large scale writes (fan-in) and reads (fan-out).

#### Various Workloads

DL supports various workloads from *latency-sensitive* online transaction
processing (OLTP) applications (e.g. WAL for distributed database and in-memory
replicated state machines), real-time stream ingestion and computing, to
analytical processing.

#### Multi Tenant

To support a large number of logs for multi-tenants, DL is designed for I/O
isolation in real-world workloads.

#### Layered Architecture

DL has a modern layered architecture design, which separates the *stateless
service tier* from the *stateful storage tier*. To support large scale writes (fan-
in) and reads (fan-out), DL allows scaling storage independent of scaling CPU
and memory.

## First Steps

* *Concepts*: Start with the [basic concepts](http://distributedlog.incubator.apache.org/docs/latest/basics/introduction) of DistributedLog.
  This will help you to fully understand the other parts of the documentation,
  including [setup](http://distributedlog.incubator.apache.org/docs/latest/deployment/cluster),
  [integration](http://distributedlog.incubator.apache.org/docs/latest/user_guide/api/main.html) and
  [operation guide](http://distributedlog.incubator.apache.org/docs/latest/admin_guide/main.html).
  It is highly recommended to read this first.
* *Quickstarts*: [Run DistributedLog](http://distributedlog.incubator.apache.org/docs/latest/start/quickstart) on your local machine
  or follow the tutorial to [write a simple program](http://distributedlog.incubator.apache.org/docs/latest/tutorials/basic-1) to interact with _DistributedLog_.
* *Setup*: The [docker](http://distributedlog.incubator.apache.org/docs/latest/deployment/docker) and [cluster](http://distributedlog.incubator.apache.org/docs/latest/deployment/cluster) setup guides show how to deploy DistributedLog stack.
* *User Guide*: You can checkout our guides about the [basic concepts](http://distributedlog.incubator.apache.org/docs/latest/basics/introduction) and the [Core Library API](http://distributedlog.incubator.apache.org/docs/latest/user_guide/api/core) or [Proxy Client API](http://distributedlog.incubator.apache.org/docs/latest/user_guide/api/proxy)
  to learn how to use DistributedLog to build your reliable real-time services.

## Next Steps

* *Design Documents*: Learn about the [architecture](http://distributedlog.incubator.apache.org/docs/latest/user_guide/architecture/main),
  [design considerations](http://distributedlog.incubator.apache.org/docs/latest/user_guide/design/main) and 
  [implementation details](http://distributedlog.incubator.apache.org/docs/latest/user_guide/implementation/main) of DistributedLog.
* *Tutorials*: You can check out the [tutorials](http://distributedlog.incubator.apache.org/docs/latest/tutorials/main) on how to build real applications.
* *Admin Guide*: You can check out our guides about how to [operate](http://distributedlog.incubator.apache.org/docs/latest/admin_guide/main) the DistributedLog Stack.

## Get In Touch

### Report a Bug

For filing bugs, suggesting improvements, or requesting new features, help us out by [opening a jira](https://issues.apache.org/jira/browse/DL).

### Need Help?

[Subscribe](dev-subscribe@distributedlog.incubator.apache.org) or [mail](dev@distributedlog.incubator.apache.org) the [dev@distributedlog.incubator.apache.org](dev@distributedlog.incubator.apache.org) list - Ask questions, find answers, join developement discussions and also help other users.

## Contributing

We feel that a welcoming open community is important and welcome contributions.

### Contributing Code

1. See [Developer Guide](https://cwiki.apache.org/confluence/display/DL/Developer+Guide) to get your local environment setup.

2. Take a look at our [open issues](https://issues.apache.org/jira/browse/DL).

3. Review our [coding style](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65867477) and follow our [code reviews](https://github.com/apache/incubator-distributedlog/pulls) to learn about our conventions.

4. Make your changes according to our [code review workflow](https://cwiki.apache.org/confluence/display/DL/Contributing+to+DistributedLog#ContributingtoDistributedLog-ContributingCodeChanges).

5. Checkout the list of [project ideas](https://cwiki.apache.org/confluence/display/DL/Project+Ideas) to contribute more features or improvements.

### Improving Website and Documentation

1. See [website/README.md](/website/README.md) on how to build the website.

2. See [docs/README.md](/docs/README.md) on how to build the documentation.

## About

Apache DistributedLog is an open source project of The Apache Software Foundation (ASF). The Apache DistributedLog project originated from [Twitter](https://twitter.com/).
