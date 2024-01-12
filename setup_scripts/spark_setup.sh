cd /root
apt update
apt install -y openjdk-17-jre-headless git pip wget
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 spark
rm spark-3.5.0-bin-hadoop3.tgz