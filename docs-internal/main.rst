Runbook
=======

.. Note::
   This page can also be accessed via `go/DistributedLog <http://go/DistributedLog>`__.

Getting Help
------------

.. Note::
    For production issues, please escalate through the on-call tree
    listed at `go/OnCall <http://go/OnCall>`__.  When in doubt, use
    the TCC HipChat room.

The Messaging Team is on HipChat in the `messaging` room.  Direct your
questions here if they're urgent.

EventBus JIRA tickets should be filled in the
`PUBSUB <https://jira.twitter.biz/browse/PUBSUB>`__ project with
component *DistributedLog*.

Runbook
-------

* `DistributedLog runbook <http://go/runbook/distributedlog>`__
* `BookKeeper runbook <http://go/runbook/bookkeeper>`__
* `Quantum Leap Troubleshooting <https://docs.google.com/document/d/1-hGj1uQncMfuL3zd113Pnv3oOmKNTsqIP8-8MBahyU8/edit#heading=h.99rqa082yxx1>`__

In June 2015, we gave a DistributedLog Tech Talk:

* `video <https://video.twitter.biz/videos/video/5680/>`__
* `slides <https://docs.google.com/a/twitter.com/presentation/d/1TySsgWjkV8lE5pjHc_KpoxcS1yGyZjYqijIeOf-WBIo/edit?usp=sharing>`__

Design Document
---------------

The design document for DistributedLog / BookKeeper is available `here
<http://go/distributedlog_architecture>`__.

Tools
-----

`dlog <https://confluence.twitter.biz/display/RUNTIMESYSTEMS/dlog>`__ is a CLI tool (written in java, distributed as a java binary package) for inspecting dl streams' metadata & data.

Tests
-----

There's a couple of jobs in Jenkins to run the tests

* `main <https://ci.twitter.biz/job/distributedlog/>`__
* `with coverage <https://ci.twitter.biz/job/distributedlog-cobertura/>`__

Team
----

* `Jordan Bull <https://birdhouse.twitter.biz/people/profile/jbull>`__ (SWE)
* `Franck Cuny <https://birdhouse.twitter.biz/people/profile/fcuny>`__ (SRE)
* `Sijie Guo <https://birdhouse.twitter.biz/people/profile/sijieg>`__ (SWE)
* `David Helder <https://birdhouse.twitter.biz/people/profile/david>`__ (EM)
* `David Rusek <https://birdhouse.twitter.biz/people/profile/drusek>`__ (SWE)
* `Leigh Stewart <https://birdhouse.twitter.biz/people/profile/lstewart>`__ (SWE)

Jira
~~~~
* `Project <https://jira.twitter.biz/browse/PUBSUB/component/16860/?selectedTab=com.atlassian.jira.jira-projects-plugin:component-summary-panel>`__
* `RapidBoard <https://jira.twitter.biz/secure/RapidBoard.jspa?rapidView=881&quickFilter=12445>`__
* `Ongoing Backlog <https://jira.twitter.biz/browse/PUBSUB-3286>`__
* `Unscheduled <https://jira.twitter.biz/browse/PUBSUB-2405>`__
