---
layout: default
---
<!-- alert -->
<div class="alert alert-info alert-dismissible" role="alert">
<span class="glyphicon glyphicon-flag" aria-hidden="true"></span>
<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>
The Apache DistributedLog project is in the process of bootstrapping. This includes the website -- so please file issues you find in <a href="/community">Jira</a>. Thanks!
</div>

<!-- landing page -->
<div class="jumbotron">
    <img class="img-responsive center-block" src="{{ "/images/distributedlog_logo_l.png" | prepend: site.baseurl }}" alt="Apache DistributedLog" />
    <p class="text-center">
        A <strong>high-throughput</strong>, <strong>low-latency</strong> replicated log service, offering <strong>durability</strong>, <strong>replication</strong> and <strong>strong consistency</strong> as essentials for building reliable <strong>real-time</strong> applications.
    </p>
    <div class="row">
        <div class="text-center">
            <a href="{{ site.baseurl }}/docs/latest/start/download" class="btn btn-primary btn-lg">Download</a>
            <a href="{{ site.baseurl }}/docs/latest/start/quickstart" class="btn btn-primary btn-lg">Quick Start</a>
        </div>
    </div>
</div>

<div class="row">
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-flash"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            High Performance
            </a>
        </h3>
        <p>
DL is able to provide <strong>milliseconds</strong> latency on <strong>durable</strong> writes with a large number
of concurrent logs, and handle high volume reads and writes per second from
thousands of clients.
        </p>
    </div>
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-menu-hamburger"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            Durable and Consistent 
            </a>
        </h3>
        <p>
Messages are persisted on disk and replicated to store multiple copies to
prevent data loss. They are guaranteed to be consistent among writers and
readers in terms of <strong>strict ordering</strong>.
        </p>
    </div>
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-random"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            Efficient Fan-in and Fan-out
            </a>
        </h3>
        <p>
DL provides an efficient service layer that is optimized for running in a multi-
tenant datacenter environment such as <i>Mesos</i> or <i>Yarn</i>. The service layer is able
to support large scale writes (fan-in) and reads (fan-out).
        </p>
    </div>
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-fire"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            Various Workloads
            </a>
        </h3>
        <p>
DL supports various workloads from <strong>latency-sensitive</strong> online transaction
processing (OLTP) applications (e.g. WAL for distributed database and in-memory
replicated state machines), real-time stream ingestion and computing, to
analytical processing.
        </p>
    </div>
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-user"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            Multi Tenant
            </a>
        </h3>
        <p>
To support a large number of logs for multi-tenants, DL is designed for I/O
isolation in real-world workloads.
        </p>
    </div>
    <div class="col-lg-4">
        <h3>
            <span class="glyphicon glyphicon-send"></span>
            <a href="{{ site.baseurl }}/docs/latest/user_guide/architecture/main.html">
            Layered Architecture
            </a>
        </h3>
        <p>
DL has a modern layered architecture design, which separates the <strong>stateless
service tier</strong> from the <strong>stateful storage tier</strong>. To support large scale writes (fan-
in) and reads (fan-out), DL allows scaling storage independent of scaling CPU
and memory.
        </p>
    </div>
</div>

<hr>
<div class="row">
  <div class="col-md-6">
    <h3>Blog</h3>
    <div class="list-group">
    {% for post in site.posts %}
    <a class="list-group-item" href="{{ post.url | prepend: site.baseurl }}">{{ post.date | date: "%b %-d, %Y" }} - {{ post.title }}</a>
    {% endfor %}
    </div>
  </div>
  <div class="col-md-6">
    <a class="twitter-timeline" href="https://twitter.com/distributedlog">Tweets by @DistributedLog</a> <script async src="//platform.twitter.com/widgets.js" charset="utf-8"></script>
  </div>
</div>

<hr>

<p style="text-align:center"><img align="center" src="https://incubator.apache.org/images/apache-incubator-logo.png" alt="Apache Incubator Logo"></p>

Apache DistributedLog is an effort undergoing incubation at [The Apache Software Foundation (ASF)](http://www.apache.org) sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.


Apache DistributedLog (incubating) is available under [Apache License, version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
