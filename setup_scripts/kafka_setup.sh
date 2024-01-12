cd /root
apt update
apt install -y openjdk-17-jre-headless git pip wget
wget https://dlcdn.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
mv kafka_2.13-3.6.1 kafka
rm kafka_2.13-3.6.1.tgz
mkdir /opt/kafka-logs
mkdir /opt/zookeeper

# edit zookeeper.properties to have data dir set to /opt/zookeeper
# edit server.properties to have data dir set to /opt/kafka-logs
# on secondary nodes, edit server.properties to have unique ids and point to main for zookeeper
# startup (main is both, secondary only the second):
# /root/kafka/bin/zookeeper-server-start.sh /root/kafka/config/zookeeper.properties &
# /root/kafka/bin/kafka-server-start.sh /root/kafka/config/server.properties &
