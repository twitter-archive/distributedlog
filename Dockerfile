FROM java:8

MAINTAINER Arvind Kandhare (arvind.kandhare@emc.com)

COPY . /opt/distributedlog-trunk/

ENV BROKER_ID 0
ENV ZK_SERVERS zk1:2181

EXPOSE 3181 
EXPOSE 9001 
EXPOSE 4181 
EXPOSE 20001

#CMD ["/bin/bash", "-c", "/opt/distributedlog-trunk/dist/release/vagrant/bk.sh", "$BROKER_ID", "127.0.0.1", "$ZK_SERVERS" ]
ENTRYPOINT [ "/opt/distributedlog-trunk/dist/release/vagrant/bk_docker_wrapper.sh" ]
 


