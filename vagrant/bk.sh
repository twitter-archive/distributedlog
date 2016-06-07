#!/bin/bash
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


# Usage: bk.sh <broker ID> <public hostname or IP> <list zookeeper public hostname or IP + port>

set -e

BROKER_ID=$1
PUBLIC_ADDRESS=$2
PUBLIC_ZOOKEEPER_ADDRESSES=$3
JMX_PORT=$4

log_dir=/opt/distributedlog-trunk/dist/release
cd $log_dir

#Stop the running bookie
echo "Killing server"
./distributedlog-service/bin/dlog-daemon.sh stop bookie

cp ./distributedlog-service/conf/bookie.conf.template ./distributedlog-service/conf/bookie-$BROKER_ID.conf

sed \
    -e 's/zkServers=localhost:2181/'zkServers=$PUBLIC_ZOOKEEPER_ADDRESSES'/' \
    ./distributedlog-service/conf/bookie.conf.template > ./distributedlog-service/conf/bookie-$BROKER_ID.conf

sleep 5 

if [ $BROKER_ID -eq "1" ]; then
 echo "create /messaging" | ./distributedlog-service/bin/dlog zkshell $PUBLIC_ZOOKEEPER_ADDRESSES
 echo "create /messaging/bookkeeper" | ./distributedlog-service/bin/dlog zkshell $PUBLIC_ZOOKEEPER_ADDRESSES 
 echo "create /messaging/bookkeeper/ledgers" | ./distributedlog-service/bin/dlog zkshell $PUBLIC_ZOOKEEPER_ADDRESSES 
 echo "Metafirmatting bookie"
 export BOOKIE_CONF=$log_dir/distributedlog-service/conf/bookie-$BROKER_ID.conf 
 echo "Y" |  ./distributedlog-service/bin/dlog bkshell metaformat
fi

echo "Configuring bookkeeper"
BOOKIE_CONF=$log_dir/distributedlog-service/conf/bookie-$BROKER_ID.conf ./distributedlog-service/bin/dlog bkshell bookieformat

echo "Starting server"
SERVICE_PORT=3181 ./distributedlog-service/bin/dlog-daemon.sh start bookie -c $log_dir/distributedlog-service/conf/bookie-$BROKER_ID.conf


if [ $BROKER_ID -eq "1" ]; then
./distributedlog-service/bin/dlog admin bind -dlzr $PUBLIC_ZOOKEEPER_ADDRESSES -dlzw $PUBLIC_ZOOKEEPER_ADDRESSES -s $PUBLIC_ZOOKEEPER_ADDRESSES -bkzr $PUBLIC_ZOOKEEPER_ADDRESSES \
      -l /messaging/bookkeeper/ledgers -i false -r true -c distributedlog://$PUBLIC_ZOOKEEPER_ADDRESSES/messaging/distributedlog/mynamespace
fi
WP_NAMESPACE=distributedlog://$PUBLIC_ZOOKEEPER_ADDRESSES/messaging/distributedlog/mynamespace WP_SHARD_ID=$BROKER_ID WP_SERVICE_PORT=4181 WP_STATS_PORT=20001 ./distributedlog-service/bin/dlog-daemon.sh start writeproxy
