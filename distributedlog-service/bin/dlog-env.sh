#!/usr/bin/env bash
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

# we need the DLog URI to be set
if [[ -z "${DISTRIBUTEDLOG_URI}" ]]; then
  echo "Environment variable DISTRIBUTEDLOG_URI is not set."
  exit 1
fi

# add the jars from current dir to the class path (should be distributedlog-service)
for i in ./*.jar; do
  CLASSPATH="$i:${CLASSPATH}"
done

# add all the jar from lib/ to the class path
for i in ./lib/*.jar; do
  CLASSPATH="$i:${CLASSPATH}"
done
