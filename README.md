[![Build Status](https://travis-ci.org/twitter/distributedlog.svg?branch=master)](https://travis-ci.org/twitter/distributedlog)

## Overview

DistributedLog (DL) is a high-performance, replicated log service, offering durability, replication and strong consistency as essentials for building reliable distributed systems.

#### High Performance

DL is able to provide milliseconds latency on durable writes with a large number of concurrent logs, and handle high volume reads and writes per second from thousands of clients.

#### Durable and Consistent

Messages are persisted on disk and replicated to store multiple copies to prevent data loss. They are guaranteed to be consistent among writers and readers in terms of strict ordering.

#### Efficient Fan-in and Fan-out

DL provides an efficient service layer that is optimized for running in a multi-tenant datacenter environment such as Mesos or Yarn. The service layer is able to support large scale writes (fan-in) and reads (fan-out).

#### Various Workloads

DL supports various workloads from latency-sensitive online transaction processing (OLTP) applications (e.g. WAL for distributed database and in-memory replicated state machines), real-time stream ingestion and computing, to analytical processing.

#### Multi Tenant

To support a large number of logs for multi-tenants, DL is designed for I/O isolation in real-world workloads.

#### Layered Architecture

DL has a modern layered architecture design, which separates the stateless service tier from the stateful storage tier. To support large scale writes (fan-in) and reads (fan-out), DL allows scaling storage independent of scaling CPU and memory.
