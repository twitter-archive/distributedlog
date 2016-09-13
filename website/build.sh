#!/bin/bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

DLOG_ENV=$1

OVERRIDED_CONFIG=_config-${DLOG_ENV}.yml

BINDIR=`dirname "$0"`
DLOG_HOME=`cd $BINDIR/.. > /dev/null;pwd`

if [ ! -d "${DLOG_HOME}/website/docs" ]; then
  mkdir ${DLOG_HOME}/website/docs
fi

if [ ! -d "${DLOG_HOME}/website/docs/latest" ]; then
  ln -s ../../docs ${DLOG_HOME}/website/docs/latest
fi

mkdir -p ${DLOG_HOME}/content/docs/latest

# build the website

cd ${DLOG_HOME}/website

bundle exec jekyll build --destination ${DLOG_HOME}/content --config _config.yml,${OVERRIDED_CONFIG}

# build the documents

DOC_HOME="${DLOG_HOME}/website/docs/latest"

cd ${DOC_HOME}

bundle exec jekyll build --destination ${DLOG_HOME}/content/docs/latest --config _config.yml,${OVERRIDED_CONFIG}
