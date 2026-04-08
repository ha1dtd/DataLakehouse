#!/bin/bash

echo "=== BASIC INPUT ==="

read -p "Namenode IP: " NN_IP
read -p "Datanode1 IP: " DN1_IP
read -p "Datanode2 IP: " DN2_IP

read -p "Datanode username: " DN_USER
read -p "Is username same on all nodes? (y/n): " SAME_USER

if [ "$SAME_USER" == "y" ]; then
    NN_USER=$(whoami)
    BASE_HOME="/home/$NN_USER"
else
    read -p "Namenode username: " NN_USER
    BASE_HOME="/home/$NN_USER"
fi

HADOOP_HOME="$BASE_HOME/hadoop"
SPARK_HOME="/opt/spark"

echo "=== INSTALL BASE DEPENDENCIES ==="
sudo apt update
sudo apt install -y wget gpg ssh pdsh python3-venv python3-pip

echo "=== INSTALL JAVA 11 ==="
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg

echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | sudo tee /etc/apt/sources.list.d/adoptium.list

sudo apt update
sudo apt install temurin-11-jdk -y

JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64

echo "=== INSTALL HADOOP ==="
cd ~

wget -4 https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 $HADOOP_HOME

echo "=== INSTALL SPARK ==="
wget -4 https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
tar -xvzf spark-3.5.8-bin-hadoop3.tgz
sudo mv spark-3.5.8-bin-hadoop3 $SPARK_HOME

echo "=== ENV SETUP ==="
cat <<EOT >> ~/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
EOT

source ~/.bashrc

echo "=== /etc/hosts ==="
sudo bash -c "cat >> /etc/hosts" <<EOT
$NN_IP namenode
$DN1_IP datanode1
$DN2_IP datanode2
EOT

echo "=== SSH KEY SETUP (GCP SAFE) ==="

ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa <<< y
PUB_KEY=$(cat ~/.ssh/id_rsa.pub)

for node in datanode1 datanode2
do
    echo "Injecting key into $node via gcloud..."

    gcloud compute ssh $DN_USER@$node --command "
        mkdir -p ~/.ssh &&
        chmod 700 ~/.ssh &&
        echo '$PUB_KEY' >> ~/.ssh/authorized_keys &&
        chmod 600 ~/.ssh/authorized_keys
    "
done

echo "=== VERIFY SSH ==="
ssh $DN_USER@$DN1_IP "echo OK"
ssh $DN_USER@$DN2_IP "echo OK"

echo "=== DISTRIBUTE HADOOP ==="
scp -r $HADOOP_HOME $DN_USER@$DN1_IP:$BASE_HOME/
scp -r $HADOOP_HOME $DN_USER@$DN2_IP:$BASE_HOME/

echo "=== DISTRIBUTE SPARK ==="
scp -r $SPARK_HOME $DN_USER@$DN1_IP:/opt/
scp -r $SPARK_HOME $DN_USER@$DN2_IP:/opt/

echo "=== REMOTE ENV SETUP ==="
for node in $DN1_IP $DN2_IP
do
ssh $DN_USER@$node <<EOF
cat <<EOT >> ~/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$BASE_HOME/hadoop
export SPARK_HOME=/opt/spark
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
EOT
source ~/.bashrc
EOF
done

echo "=== HADOOP CONFIG ==="

cat <<EOT > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://namenode:9000</value>
</property>
</configuration>
EOT

cat <<EOT > $HADOOP_HOME/etc/hadoop/workers
datanode1
datanode2
EOT

mkdir -p $HADOOP_HOME/data/namenode
mkdir -p $HADOOP_HOME/data/datanode

echo "=== YARN CONFIG ==="
cat <<EOT > $HADOOP_HOME/etc/hadoop/yarn-site.xml
<configuration>
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>namenode</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
</configuration>
EOT

echo "=== INSTALL MINIO ==="
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/

mkdir -p ~/minio-data

echo "=== INSTALL AIRFLOW ==="
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install --upgrade pip

AIRFLOW_VERSION=2.9.0
PYTHON_VERSION=3.11

pip install "apache-airflow==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

echo "=== SETUP COMPLETE ==="