---
title: Docker
top-nav-group: deployment
top-nav-pos: 2
top-nav-title: Docker
layout: default
---

.. contents:: This page provides instructions on how to deploy **DistributedLog** using docker.

Docker Setup
============

Prerequesites
-------------
1. Docker

Steps
-----
1. Create a snapshot using

.. code-block:: bash

    ./scripts/snapshot


2. Create your own docker image using 

.. code-block:: bash

    docker build -t <your image name> .


3. You can run the docker container using

.. code-block:: bash

    docker run -e ZK_SERVERS=<zk server list> -e DEPLOY_BK=<true|false> -e DEPLOY_WP=<true|false> <your image name>


Environment variables
---------------------

Following are the environment variables which can change how the docker container runs.

1. **ZK_SERVERS**: ZK servers running exernally (the container does not run a zookeeper)
2. **DEPLOY_BOTH**: Deploys writeproxies as well as the bookies
3. **DEPLOY_WP**: Flag to notify that a writeproxy needs to be deployed
4. **DEPLOY_BK**: Flag to notify that a bookie needs to be deployed
