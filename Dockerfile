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
FROM java:8

MAINTAINER Arvind Kandhare [arvind.kandhare@emc.com]

COPY . /opt/distributedlog-trunk/

ENV BROKER_ID 0
ENV ZK_SERVERS zk1:2181

EXPOSE 3181 
EXPOSE 9001 
EXPOSE 4181 
EXPOSE 20001

#CMD ["/bin/bash", "-c", "/opt/distributedlog-trunk/dist/release/vagrant/bk.sh", "$BROKER_ID", "127.0.0.1", "$ZK_SERVERS" ]
ENTRYPOINT [ "/opt/distributedlog-trunk/dist/release/vagrant/bk_docker_wrapper.sh" ]
 


