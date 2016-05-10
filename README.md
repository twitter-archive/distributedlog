## Overview

DistributedLog (DL) is a high-performance, replicated log service, offering durability, replication and strong consistency
as essentials for building reliable distributed systems.

#### High Performance

DL is able to provide milliseconds latency on durable writes with large number of concurrent logs.
Also also handle high volume reads and writes per second from thousands of clients.

#### Durable and Consistent

Messages are persisted on disk and replicated to store multiple copies to prevent data loss.
They are guaranteed to be consistent among writers and readers in terms of strict ordering.

#### Efficient Fan-in and Fan-out

DL provides an efficient serving layer that optimized for running in a multi-tenant datacenter
environment such as Mesos or Yarn. The serving layer is able to support large scale writes fan-in
and reads fan-out.

#### Various Workloads

DL supports various workloads from latency sensitive online transaction processing (OLTP) applications
(e.g. WAL for distributed database and in-memory replicated state machines), real-time stream ingestion
and computing to analytical processing.

#### Multi Tenant

DL is designed for I/O isolation in real-world workloads, to support large scale of number of logs for multi
tenants.

#### Layered Architecture

DL has a modern layered-architecture design, that separates stateless serving tier from stateful storage tier.
It allows scaling storage independent of scaling cpu and memory, to support large scale writes fan-in and
reads fan-out.
