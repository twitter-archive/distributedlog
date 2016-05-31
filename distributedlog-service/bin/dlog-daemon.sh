#!/usr/bin/env bash
#
#/**
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

usage() {
    cat <<EOF
Usage: dlog-daemon.sh (start|stop) <service> <args...>
where service is one of:
    zookeeper                   Run the zookeeper server
    bookie                      Run the bookie server
    bookie-rereplicator         Run the bookie rereplicator
    writeproxy                  Run the write proxy server
    writeproxy-monitor          Run the write proxy monitor

where argument is one of:
    -force (accepted only with stop service): Decides whether to stop the process forcefully if not stopped by normal shutdown
EOF
}

BINDIR=`dirname "$0"`
DL_HOME=`cd $BINDIR/..;pwd`

if [ -f $DL_HOME/conf/dlogenv.sh ]
then
 . $DL_HOME/conf/dlogenv.sh
fi

SERVICE_PORT=${SERVICE_PORT:-"0"}
SERVICE_ARGS=""

# DLOG logging configuration
DLOG_LOG_DIR=${DLOG_LOG_DIR:-"$DL_HOME/logs"}
DLOG_ROOT_LOGGER=${DLOG_ROOT_LOGGER:-'INFO,R'}
# Process Control Parameters
DLOG_STOP_TIMEOUT=${DLOG_STOP_TIMEOUT:-30}
DLOG_PID_DIR=${DLOG_PID_DIR:-$DL_HOME/pids}

if [ $# -lt 2 ]
then
    echo "Error: no enough arguments provided."
    usage
    exit 1
fi

command=$1
shift
service=$1
shift

service_class=$service
case $service in
    (zookeeper)
        service_class="org.apache.zookeeper.server.quorum.QuorumPeerMain"
        DLOG_ROOT_LOGGER=${ZK_ROOT_LOGGER:-'INFO,R'}
        ;;
    (bookie)
        service_class="org.apache.bookkeeper.proto.BookieServer"
        DLOG_ROOT_LOGGER=${BK_ROOT_LOGGER:-'INFO,R'}
        ;;
    (bookie-rereplicator)
        service_class="org.apache.bookkeeper.replication.AutoRecoveryMain"
        DLOG_ROOT_LOGGER=${BK_ROOT_LOGGER:-'INFO,R'}
        ;;
    (writeproxy)
        service_class="com.twitter.distributedlog.service.DistributedLogServerApp"
        DLOG_ROOT_LOGGER=${WP_ROOT_LOGGER:-'INFO,R'}
        WP_CONF_FILE=${WP_CONF_FILE:-"$DL_HOME/conf/write_proxy.conf"}
        WP_SERVICE_PORT=${WP_SERVICE_PORT:-'4181'}
        WP_STATS_PORT=${WP_STATS_PORT:-'9000'}
        WP_STATS_PROVIDER=${WP_STATS_PROVIDER:-'org.apache.bookkeeper.stats.CodahaleMetricsServletProvider'}
        WP_SHARD_ID=${WP_SHARD_ID:-'0'}
        WP_NAMESPACE=${WP_NAMESPACE:-'distributedlog://127.0.0.1:2181/messaging/distributedlog/mynamespace'}
        SERVICE_PORT=${WP_SERVICE_PORT}
        SERVICE_ARGS="--conf ${WP_CONF_FILE} --uri ${WP_NAMESPACE} --shard-id ${WP_SHARD_ID} --port ${WP_SERVICE_PORT} --stats-port ${WP_STATS_PORT} --stats-provider ${WP_STATS_PROVIDER} --announce --thriftmux"
        DLOG_EXTRA_OPTS="${DLOG_EXTRA_OPTS} -DcodahaleStatsHttpPort=${WP_STATS_PORT} -Dserver_port=${WP_SERVICE_PORT} -Dserver_shard=${WP_SHARD_ID}"
        ;;
    (writeproxy-monitor)
        ;;
    (*)
        echo "Error: unknown service name $service"
        usage
        exit 1
        ;;
esac

echo "doing $command $service ..."

export DLOG_LOG_DIR=$DLOG_LOG_DIR
export DLOG_ROOT_LOGGER=$DLOG_ROOT_LOGGER
export DLOG_LOG_FILE=dlog-$service-$HOSTNAME-$SERVICE_PORT.log
export DLOG_EXTRA_OPTS=$DLOG_EXTRA_OPTS

pid=$DLOG_PID_DIR/dlog-$service-$SERVICE_PORT.pid
out=$DLOG_LOG_DIR/dlog-$service-$HOSTNAME-$SERVICE_PORT.out
logfile=$DLOG_LOG_DIR/$DLOG_LOG_FILE

rotate_out_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
       num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

mkdir -p "$DLOG_LOG_DIR"
mkdir -p "$DLOG_PID_DIR"

case $command in
  (start)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $service running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    rotate_out_log $out
    echo starting $service, logging to $logfile
    dlog=$DL_HOME/bin/dlog
    nohup $dlog $service_class ${SERVICE_ARGS} "$@" > "$out" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head $out
    sleep 2;
    if ! ps -p $! > /dev/null ; then
      exit 1
    fi
    ;;

  (stop)
    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $service
        kill $TARGET_PID

        count=0
        location=$DLOG_LOG_DIR
        while ps -p $TARGET_PID > /dev/null;
         do
          echo "Shutdown is in progress... Please wait..."
          sleep 1
          count=`expr $count + 1`
         
          if [ "$count" = "$DLOG_STOP_TIMEOUT" ]; then
                break
          fi
         done
        
        if [ "$count" != "$DLOG_STOP_TIMEOUT" ]; then
            echo "Shutdown completed."
        fi
                 
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
              fileName=$location/$service.out
              $JAVA_HOME/bin/jstack $TARGET_PID > $fileName
              echo Thread dumps are taken for analysis at $fileName
              if [ "$1" == "-force" ]
              then
                 echo forcefully stopping $service
                 kill -9 $TARGET_PID >/dev/null 2>&1
                 echo Successfully stopped the process
              else
                 echo "WARNNING : $service is not stopped completely."
                 exit 1
              fi
        fi
      else
        echo no $service to stop
      fi
      rm $pid
    else
      echo no $service to stop
    fi
    ;;

  (*)
    usage
    echo $supportedargs
    exit 1
    ;;
esac
