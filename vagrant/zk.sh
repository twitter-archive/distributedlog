# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# Usage: zk.sh <zkid> <num_zk>

set -e

ZKID=$1
NUM_ZK=$2
JMX_PORT=$3
log_dir=/opt/distributedlog-trunk/dist/release
cd $log_dir

cp $log_dir/distributedlog-service/conf/zookeeper.conf.template $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
echo "initLimit=5" >> $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
echo "syncLimit=2" >> $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
echo "quorumListenOnAllIPs=true" >> $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
sed  '/server.1/d' \
    ./distributedlog-service/conf/bookie.conf.template > ./distributedlog-service/conf/zookeeper-$ZKID.conf
for i in `seq 1 $NUM_ZK`; do
    echo "server.${i}=zk${i}:2888:3888" >>$log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
done

mkdir -p /tmp/data/zookeeper
echo "$ZKID" > /tmp/data/zookeeper/myid

echo "Killing ZooKeeper"
./distributedlog-service/bin/dlog-daemon.sh stop zookeeper || true
sleep 5
echo "Starting ZooKeeper"
echo "./distributedlog-service/bin/dlog-daemon.sh start zookeeper $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties"
./distributedlog-service/bin/dlog-daemon.sh start zookeeper $log_dir/distributedlog-service/conf/zookeeper-$ZKID.properties
