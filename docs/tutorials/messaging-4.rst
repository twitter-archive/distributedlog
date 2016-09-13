---
title: Tutorials - Exact-Once Processing
top-nav-group: messaging
top-nav-pos: 4
top-nav-title: Exact-Once Processing
layout: default
---

.. contents:: Messaging Tutorial - Exact-Once Processing

Exact-Once Processing
=====================

Applications typically choose between `at-least-once` and `exactly-once` processing semantics.
`At-least-once` processing guarantees that the application will process all the log records,
however when the application resumes after failure, previously processed records may be re-processed
if they have not been acknowledged. `Exactly once` processing is a stricter guarantee where applications
must see the effect of processing each record exactly once. `Exactly once` semantics can be achieved
by maintaining reader positions together with the application state and atomically updating both the
reader position and the effects of the corresponding log records. 

This tutorial_ shows how to do `exact-once` processing.

.. _tutorial: https://github.com/apache/incubator-distributedlog/blob/master/distributedlog-tutorials/distributedlog-messaging/src/main/java/com/twitter/distributedlog/messaging/StreamTransformer.java
