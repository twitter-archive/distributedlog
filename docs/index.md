---
layout: default
title: "Apache DistributedLog Documentation"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Overview

This documentation is for Apache DistributedLog version {{ site.distributedlog_version }}.

Apache DistributedLog is a high-throughput, low-latency replicated log service, offering durability,
replication and strong consistency as essentials for building reliable real-time applications. 

## First Steps

- **Concepts**: Start with the [basic concepts]({{ site.baseurl }}/basics/introduction) of DistributedLog.
  This will help you to fully understand the other parts of the documentation, including the setup, integration,
  and operation guides. It is highly recommended to read this first.

- **Quickstarts**: [Run DistributedLog]({{ site.baseurl }}/start/quickstart) on your local machine or follow the tutorial to [write a simple program]({{ site.baseurl }}/tutorials/basic-1) to interact with _DistributedLog_.

- **Setup**: The [docker]({{ site.baseurl }}/deployment/docker) and [cluster]({{ site.baseurl }}/deployment/cluster) setup guides show how to deploy DistributedLog Stack.

- **Programming Guide**: You can check out our guides about [basic concepts]({{ site.baseurl }}/basics/introduction) and the [Core Library API]({{ site.baseurl }}/user_guide/api/core) or [Proxy Client API]({{ site.baseurl }}/user_guide/api/proxy) to learn how to use DistributedLog to build your reliable real-time services.

## Next Steps

- **Design Documents**: Learn about the [architecture]({{ site.baseurl }}/user_guide/architecture/main), [design considerations]({{ site.baseurl }}/user_guide/design/main) and [implementation details]({{ site.baseurl }}/user_guide/implementation/main) of DistributedLog.

- **Tutorials**: You can check out the [tutorials]({{ site.baseurl }}/tutorials/main) on how to build real applications.

- **Operation Guide**: You can check out our guides about how to [operate]({{ site.baseurl }}/admin_guide/main) the DistributedLog Stack.
