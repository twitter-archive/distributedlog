Sample Deployment using vagrant
================================

This file explains vagrant deployment.

Prerequesites
--------------
1. Vagrant: From https://www.vagrantup.com/downloads.html
2. vagrant-hostmanager plugin: From https://github.com/devopsgroup-io/vagrant-hostmanager

Steps
-----
1. Create a snapshot using ./scripts/snapshot
2. Run vagrant up from the root directory of the enlistment
3. Vagrant brings up a zookeepers with machine names: zk1,zk2, .... with IP addresses 192.168.50.11,192.168.50.12,....
4. Vagrant brings the bookies with machine names: node1,node2, ... with IP addresses 192.168.50.51,192.168.50.52,....
5. The script will also start writeproxies at distributedlog://$PUBLIC_ZOOKEEPER_ADDRESSES/messaging/distributedlog/mynamespace as the namespace. If you want it to point to a different namespace/port/shard, please update the ./vagrant/bk.sh script. 
6. If you want to run the client on the host machine, please add these node names and their IP addresses to the /etc/hosts file on the host.
