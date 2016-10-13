---
layout: post
title:  'A Technical Review of Kafka and DistributedLog'
date:   2016-09-19 10:00:00
categories: technical-review
permalink: /technical-review/2015/09/19/kafka-vs-distributedlog
authors:
- sijie
---

We open sourced [DistributedLog](http://DistributedLog.io) [^distributedlog] in May 2016.
It generated a lot of interest in the community. One frequent question we are asked is how does DistributedLog
compare to [Apache Kafka](http://kafka.apache.org/) [^kafka]. Technically DistributedLog is not a full fledged partitioned
pub/sub system like Apache Kafka. DistributedLog is a replicated log stream store, using [Apache BookKeeper](http://bookKeeper.apache.org/) [^bookkeeper] as its log segment store.
It focuses on offering *durability*, *replication* and *strong consistency* as essentials for building reliable
real-time systems. One can use DistributedLog to build and experiment with different messaging models
(such as Queue, Pub/Sub). 

Since both of them share very similar data model around `log`, this blog post will discuss the difference
between Apache Kafka and DistributedLog from a technical perspective. We will try our best to be objective.
However, since we are not experts in Apache Kafka, we may have made wrong assumptions about Apache Kafka.
Please do correct us if that is the case.

First, let's have a quick overview of Kafka and DistributedLog.

## What is Kafka?

Kafka is a distributed messaging system originally built at Linkedin and now part of [Apache Software Foundation](http://apache.org/).
It is a partition based pub/sub system. The key abstraction in Kafka is a topic.
A topic is partitioned into multiple partitions. Each partition is stored and replicated in multiple brokers.
Producers publish their records to partitions of a topic (round-robin or partitioned by keys), and consumers
consume the published records of that topic. Data is published to a leader broker for a partition and
replicated to follower brokers; all read requests are subsequently served by the leader broker.
The follower brokers are used for redundancy and backup in case the leader can no longer serve requests.
The left diagram in Figure 1 shows the data flow in Kafka.

## What is DistributedLog?

Unlike Kafka, DistributedLog is not a partitioned pub/sub system. It is a replicated log stream store.
The key abstraction in DistributedLog is a continuous replicated log stream. A log stream is segmented
into multiple log segments. Each log segment is stored as
a [ledger](http://bookkeeper.apache.org/docs/r4.4.0/bookkeeperOverview.html) [^ledger] in Apache BookKeeper,
whose data is replicated and distributed evenly across multiple bookies (a bookie is a storage node in Apache BookKeeper).
All the records of a log stream are sequenced by the owner of the log stream - a set of write proxies that
manage the ownership of log streams [^corelibrary]. Each of the log records appended to a log stream will
be assigned a sequence number. The readers can start reading the log stream from any provided sequence number.
The read requests will be load balanced across the storage replicas of that stream.
The right diagram in Figure 1 shows the data flow in DistributedLog.

<img class="center-block" src="{{ "/images/blog/kafka-distributedlog-dataflow.png" | prepend: site.baseurl }}" alt="Kafka vs DistributedLog" width="500">
<div class="text-center">
<i>Figure 1 - Apache Kafka vs Apache DistributedLog</i>
</div>

## How do Kafka and DistributedLog differ?

For an apples-to-apples comparison, we will only compare Kafka partitions with DistributedLog streams in this blog post. The table below lists the most important differences between the two systems.

<table border="1px">
  <thead>
    <tr>
      <th>&nbsp;</th>
      <th><strong>Kafka</strong></th>
      <th><strong>DistributedLog</strong></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Data Segmentation/Distribution</strong></td>
      <td>The data of a Kafka partition lives in a set of brokers; The data is segmented into log segment files locally on each broker.</td>
      <td>The data of a DL stream is segmented into multiple log segments. The log segments are evenly distributed across the storage nodes.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Data Retention</strong></td>
      <td>The data is expired after configured retention time or compacted to keep the latest values for keys.</td>
      <td>The data can be either expired after configured retention time or explicitly truncated to given positions.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Write</strong></td>
      <td>Multiple-Writers via Broker</td>
      <td>Multiple-Writers via Write Proxy; Single-Writer via Core Library</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Read</strong></td>
      <td>Read from leader broker</td>
      <td>Read from any storage node which has the data</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Replication</strong></td>
      <td>ISR (In-Sync-Replica) Replication - Both leader and followers are brokers.</td>
      <td>Quorum vote replication - Leaders are write proxies; Followers are bookies.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Replication Repair</strong></td>
      <td>Done by adding new replica to copy data from the leader broker.</td>
      <td>The replication repair is done by bookie AutoRecovery mechanism to ensure replication factor.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Cluster Expand</strong></td>
      <td>Need to re-distribute (balance) partition when adding new brokers. Carefully track the size of each partition and make sure rebalancing doesn’t saturate network and I/O.</td>
      <td>No data redistribution when either adding write proxies or bookies; New log segment will be automatically allocated on newly added bookies.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Storage</strong></td>
      <td>File (set of files) per partition</td>
      <td>Interleaved Storage Format</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>Durability</strong></td>
      <td>Only write to filesystem page cache. Configurable to either wait for acknowledgement from leader or from all replicas in ISR.</td>
      <td>All writes are persisted to disk via explicit fsync. Wait for acknowledgements from a configurable quorum of bookies.</td>
    </tr>
  </tbody>
  <tbody>
    <tr>
      <td><strong>I/O Isolation</strong></td>
      <td>No physical I/O isolation. Rely on filesystem page cache.</td>
      <td>Physical I/O isolation. Separate journal disk for write and ledger disks for reads.</td>
    </tr>
  </tbody>
</table>

### Data Model

A Kafka partition is a log stored as a (set of) file(s) in the broker's disks.
Each record is a key/value pair (key can be omitted for round-robin publishes). 
The key is used for assigning the record to a Kafka partition and also for [log compaction](https://cwiki.apache.org/confluence/display/KAFKA/Log+Compaction) [^logcompaction].
All the data of a partition is stored only on a set of brokers, replicated from leader broker to follower brokers.

A DistributedLog stream is a `virtual` stream stored as a list of log segments.
Each log segment is stored as a BookKeeper ledger and replicated to multiple bookies.
There will be only one active log segment accepting writes at any time.
An old log segment is sealed and a new log segment will be opened when a given time period elapses,
an old log segment reaches a configured size threshold (based on configured log segment rolling policies),
or when the owner of the log stream fails. 

The differences in data segmentation and distribution of Kafka partition and DistributedLog stream result in differences in data retention policies and cluster operations (e.g. cluster expansion).

Figure 2 shows the differences between DistributedLog and Kafka data model.

<img class="center-block" src="{{ "/images/blog/kafka-distributedlog-segmentation.png" | prepend: site.baseurl }}" alt="Kafka Partition and DistributedLog Stream" width="500">
<div class="text-center">
<i>Figure 2 - Kafka Partition and DistributedLog Stream</i>
</div>

#### Data Retention

All the data of a Kafka partition is stored on one broker (replicated to other brokers). Data is expired and deleted after a configured retention period. Additionally, a Kafka partition can be configured to do log compaction to keep only the latest values for keys.

Similar to Kafka, DistributedLog also allows configuring retention periods for individual streams and expiring / deleting log segments after they are expired. Besides that, DistributedLog also provides an explicit-truncation mechanism. Application can explicitly truncate a log stream to a given position in the stream. This is important for building replicated state machines as the replicated state machines require persisting state before deleting log records.
[Manhattan](https://blog.twitter.com/2016/strong-consistency-in-manhattan) [^consistency] is one example of a system that uses this functionality.

#### Operations

The differences in data segmentation/distribution also result in different experiences in operating the cluster, one example is when expanding the cluster.

When expanding a Kafka cluster, the partitions typically need to be rebalanced. The rebalancing moves Kafka partitions around to different replicas to try to achieve an even distribution. This involves copying the entire stream data from one replica to another. It has been mentioned in several talks that rebalancing needs to be executed carefully to avoid saturating disk and network resources.

Expanding a DistributedLog cluster works in a very different way. DistributedLog consists of two layers: a storage layer - Apache BookKeeper, and a serving layer - the write and read proxies. When expanding the storage layer, we typically just add more bookies. The new bookies will be discovered by the writers of the log streams and will immediately be available for writing of new log segments. There is no rebalancing involved when expanding the data storage layer. Rebalancing only comes into play when adding capacity to the serving layer, however this rebalancing only moves the ownership of the log streams to allow network bandwidth to be evenly used across proxies, there is no data copying involved, only accounting of ownership. This separation of storage and serving layers not only makes it easy to integrate the system with any auto-scaling mechanism, but also allows scaling resources independently.

### Writer & Producer

As shown in Figure 1, Kafka producers write batches of records to the leader broker of a Kafka partition. The follower brokers in the [ISR (in-sync-replica) set](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Replication) [^kafkareplication] will replicate the records from the leader broker. A record is considered as committed only when the leader receives acknowledgments from all the replicas in the ISR. The producer can be configured to wait for the response from leader broker or from all brokers in the ISR.

There are two ways in DistributedLog to write log records to a DistributedLog stream, one is using a thin thrift client to write records through the write proxies (aka multiple-writer semantic), while the other one is using the DistributedLog core library to talk directly to the storage nodes (aka single-writer semantics). The first approach is common for building messaging systems while the second approach is common for building replicated state machines. You can check the [Best Practices](http://distributedlog.incubator.apache.org/docs/latest/user_guide/api/practice) section in DistributedLog documentation for more details about what should be used. 

The owner of a log stream writes a batch of records as BookKeeper entries in parallel to the bookies and waits for responses from a quorum of bookies. The size of a quorum is known as the `ack_quorum_size` of a BookKeeper ledger and it can be configured per DistributedLog stream. It provides similar flexibility on durability as Kafka producers. We will discuss more about the differences on replication algorithms later in 'Replication' section.

Both Kafka and DistributedLog support end-to-end batching and compression. One slight difference between Kafka and DistributedLog is all the writes in DistributedLog are flushed (via fsync) to disk before acknowledging (We are not aware of Kafka providing a similar guarantee).

### Reader & Consumer

Kafka consumers read the records from the leader broker. The assumption of this design is that the leader has all the latest data in filesystem page cache most of the time. This is a good choice to fully utilize the filesystem page cache and achieve high performance.

DistributedLog takes a different approach. As there is no distinguished `leadership` on storage nodes, DistributedLog reads the records from any of the storage nodes that store the data. In order to achieve predictable low latency, DistributedLog deploys a speculative read mechanism to retry reading from different replicas after a configured speculative read timeout. This will result in more read traffic to storage nodes than Kafka's approach. However configuring the speculative read timeout to be around the 99th percentile of read latency can help mask the latency variabilities on the storage node with an additional 1% traffic overhead.

The differences in considerations and mechanisms on reads are mostly due to the differences in replication mechanism and the I/O system on storage nodes. They will be discussed below.

### Replication

Kafka uses an ISR replication algorithm - a broker is elected as the leader. All the writes are published to the leader broker and all the followers in a ISR set will read and replicate data from the leader. The leader maintains a high watermark (HW), which is the offset of last committed record for a partition. The high watermark is continuously propagated to the followers and is checkpointed to disk in each broker periodically for recovery. The HW is updated when all replicas in ISR successfully write the records to the filesystem (not necessarily to disk) and acknowledge back to the leader. 

ISR mechanism allows adding and dropping replicas to achieve tradeoff between availability and performance. However the side effect of allowing adding and shrinking replica set is increased probability of [data loss](https://aphyr.com/posts/293-jepsen-kafka)[^jepsen].

DistributedLog uses a quorum-vote replication algorithm, which is typically seen in consensus algorithms like Zab, Raft and Viewstamped Replication. The owner of the log stream writes the records to all the storage nodes in parallel and waits until a configured quorum of storage nodes have acknowledged before they are considered to be committed. The storage nodes acknowledge the write requests only after the data has been persisted to disk by explicitly calling flush. The owner of the log stream also maintains the offset of last committed record for a log stream, which is known as LAC (LastAddConfirmed) in Apache BookKeeper. The LAC is piggybacked into entries (to save extra rpc calls) and continuously propagated to the storage nodes. The size of replica set in DistributedLog is configured and fixed per log segment per stream. The change of replication settings only affect the newly allocated log segments but not the old log segments.

### Storage

Each Kafka partition is stored as a (set of) file(s) on the broker's disks. It achieves good performance by leveraging the filesystem page cache and I/O scheduling. It is also the way Kafka leverages Java's sendfile api to get data in and out of brokers efficiently. However, it can potentially be exposed to unpredictabilities when page cache evictions become frequent due to various reasons (e.g. consumer falls behind, random writes/reads and such).

DistributedLog uses a different I/O model. Figure 3 illustrates the I/O system on the bookies (BookKeeper's storage nodes). The three common I/O workloads - writes (blue lines), tailing-reads (red lines) and catchup-reads (purple lines) are separated into 3 physically different I/O subsystems. All the writes are sequentially appended to journal files on the journal disks and group committed to the disk. After the writes are durably persisted on the disks, they are put into a memtable and acknowledged back to the clients. The data in the memtable is asynchronously flushed into an interleaved indexed data structure: the entries are appended into entry log files and the offsets are indexed by entry ids in the ledger index files. The latest data is guaranteed to be in memtable to serve tailing reads. Catch-up reads read from the entry log files. With physical isolation, a bookie node can fully utilize the network ingress bandwidth and sequential write bandwidth on the journal disk for writes and utilize the network egress bandwidth and IOPS on multiple ledger disks for reads, without interfering with each other.

<img class="center-block" src="{{ "/images/blog/kafka-distributedlog-bookkeeper-io.png" | prepend: site.baseurl }}" alt="BookKeeper I/O Isolation" width="500">
<div class="text-center">
<i>Figure 3 - BookKeeper I/O Isolation</i>
</div>

## Summary

Both Kafka and DistributedLog are designed for log stream related workloads. They share some similarities but different design principles for storage and replication lead to different implementations. Hopefully this blog post clarifies the differences and address some of the questions. We are also working on  followup blog posts to explain the performance characteristics of DistributedLog. 

## References

[^distributedlog]: DistributedLog Website: http://distributedLog.io

[^kafka]: Apache Kafka Website: http://kafka.apache.org/

[^bookkeeper]: Apache BookKeeper Website: http://bookKeeper.apache.org/

[^ledger]: BookKeeper Ledger: http://bookkeeper.apache.org/docs/r4.4.0/bookkeeperOverview.html

[^logcompaction]: Kafka Log Compaction: https://cwiki.apache.org/confluence/display/KAFKA/Log+Compaction

[^consistency]: Strong consistency in Manhattan: https://blog.twitter.com/2016/strong-consistency-in-manhattan

[^kafkareplication]: Kafka Replication: https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Replication

[^jepsen]: Jepsen: Kafka: https://aphyr.com/posts/293-jepsen-Kafka

[^corelibrary]: Applications can also use the core library directly to append log records. This is very useful for use cases like replicated state machines that require ordering and exclusive write semantics.
