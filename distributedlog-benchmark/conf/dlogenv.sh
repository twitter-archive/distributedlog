#!/bin/sh
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# default settings for starting distributedlog sandbox

# Log4j configuration file
# DLOG_LOG_CONF=

# Extra options to be passed to the jvm
# DLOG_EXTRA_OPTS=

# Add extra paths to the dlog classpath
# DLOG_EXTRA_CLASSPATH=

########################
# Benchmark Arguments
########################

# Configuration File
# BENCH_CONF_FILE=
# Stats Provider
STATS_PROVIDER=org.apache.bookkeeper.stats.CodahaleMetricsServletProvider
# Stream Name Prefix
STREAM_NAME_PREFIX=distributedlog-smoketest
# Benchmark Run Duration in minutes
BENCHMARK_DURATION=60
# DL Namespace
DL_NAMESPACE=distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace
# Benchmark SHARD id
BENCHMARK_SHARD_ID=0

# How many streams
NUM_STREAMS=100

# Max stream id (exclusively)
MAX_STREAM_ID=100

#########
# Writer
#########

# Start stream id
START_STREAM_ID=0
# End stream id (inclusively)
END_STREAM_ID=99

# Message Size
MSG_SIZE=1024

# Write Rate
# Initial rate - messages per second
INITIAL_RATE=1
# Max rate - messages per second
MAX_RATE=1000
# Rate change each interval - messages per second
CHANGE_RATE=100
# Rate change interval, in seconds
CHANGE_RATE_INTERVAL=300

##########
# Reader 
##########

### Reader could be run in a sharded way. Each shard is responsible for
### reading a subset of the streams. A stream could be configured to be
### read by multiple shards.
NUM_READERS_PER_STREAM=1

### Interval that reader issues truncate requests to truncate the streams, in seconds
TRUNCATION_INTERVAL=600
