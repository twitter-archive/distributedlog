---
title: Build DistributedLog from Source
top-nav-group: setup
top-nav-pos: 1
top-nav-title: Build DistributedLog from Source
layout: default
---

.. contents:: This page covers how to build DistributedLog {{ site.distributedlog_version }} from sources.

Build DistributedLog
====================

In order to build DistributedLog you need the source code. Either `download the source of a release`_ or `clone the git repository`_.

.. _download the source of a release: {{ site.baseurl }}/download
.. _clone the git repository: {{ site.github_url }}

In addition you need **Maven 3** and a **JDK** (Java Development Kit). DistributedLog requires **at least Java 7** to build. We recommend using Java 8.

To clone from git, enter:

.. code-block:: bash

    git clone {{ site.github_url }}


The simplest way of building DistributedLog is by running:

.. code-block:: bash

    mvn clean package -DskipTests


This instructs Maven_ (`mvn`) to first remove all existing builds (`clean`) and then create a new DistributedLog package(`package`). The `-DskipTests` command prevents Maven from executing the tests.

.. _Maven: http://maven.apache.org

Build
~~~~~

- Build all the components without running tests

.. code-block:: bash

    mvn clean package -DskipTests

- Build all the components and run all the tests

.. code-block:: bash

    mvn clean package


- Build a single component: as distributedlog is using shade plugin. shade only run when packaging so pre-install the dependencies before building a single component.

.. code-block:: bash

    mvn clean install -DskipTests
    mvn -pl :<module-name> package [-DskipTests] // example: mvn-pl :distributedlog-core package


- Test a single class: as distributedlog is using shade plugin. shade only run when packaging so pre-install the dependencies before building a single component.

.. code-block:: bash

    mvn clean install -DskipTests
    mvn -pl :<module-name> clean test -Dtest=<test-class-name>


Scala Versions
~~~~~~~~~~~~~~

DistributedLog has dependencies such as `Twitter Util`_, Finagle_ written in Scala_. Users of the Scala API and libraries may have to match the Scala version of DistributedLog with the Scala version of their projects (because Scala is not strictly backwards compatible).

.. _Twitter Util: https://twitter.github.io/util/
.. _Finagle: https://twitter.github.io/finagle/
.. _Scala: http://scala-lang.org

**By default, DistributedLog is built with the Scala 2.11**. To build DistributedLog with Scala *2.10*, you can change the default Scala *binary version* with the following script:

.. code-block:: bash

    # Switch Scala binary version between 2.10 and 2.11
    tools/change-scala-version.sh 2.10
    # Build with Scala version 2.10
    mvn clean install -DskipTests


DistributedLog is developed against Scala *2.11* and tested additionally against Scala *2.10*. These two versions are known to be compatible. Earlier versions (like Scala *2.9*) are *not* compatible.

Newer versions may be compatible, depending on breaking changes in the language features used by DistributedLog's dependencies, and the availability of DistributedLog's dependencies in those Scala versions.
