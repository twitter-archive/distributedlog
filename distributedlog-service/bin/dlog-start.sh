#!/bin/bash

DLOGBIN="${BASH_SOURCE-$0}"
DLOGBIN="$(dirname "${DLOGBIN}")"
DLOGBINDIR="$(cd "${DLOGBIN}"; pwd)"

. "${DLOGBINDIR}"/dlog-env.sh

java -cp "${CLASSPATH}" \
     -Dlog4j.configuration=conf/log4j.properties \
     -DstatsHttpPort=9000 -DstatsExport=true \
     -Dserver_shard=0 \
     com.twitter.distributedlog.service.DistributedLogServerApp \
     --port 8000 \
     --uri "${DISTRIBUTEDLOG_URI}" \
     --conf conf/distributedlog.conf
