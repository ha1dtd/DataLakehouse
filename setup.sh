#!/bin/bash

echo "=== FOXAI CLUSTER SETUP ==="

# --- USER INPUT ---
read -p "Namenode PUBLIC IP: " NN_PUBLIC_IP
read -p "Namenode PRIVATE IP: " NN_PRIVATE_IP
read -p "Datanode1 PRIVATE IP: " DN1_PRIVATE_IP
read -p "Datanode2 PRIVATE IP: " DN2_PRIVATE_IP

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
JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"

# --- SYSTEM DEPENDENCIES ---
echo "=== INSTALL BASE DEPENDENCIES ==="
sudo apt update
sudo apt install -y wget gpg ssh pdsh python3-venv python3-pip curl tar

# --- JAVA 11 INSTALL ---
echo "=== INSTALL JAVA 11 ==="
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg
echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install -y temurin-11-jdk

# --- HADOOP INSTALL ---
echo "=== INSTALL HADOOP ==="
cd $BASE_HOME
wget -4 https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 $HADOOP_HOME

# --- SPARK INSTALL ---
echo "=== INSTALL SPARK ==="
wget -4 https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
tar -xvzf spark-3.5.8-bin-hadoop3.tgz
sudo mv spark-3.5.8-bin-hadoop3 $SPARK_HOME
sudo chown -R $NN_USER:$NN_USER $SPARK_HOME

# --- ENV SETUP ---
echo "=== SETUP ENVIRONMENT VARIABLES ==="
cat <<EOT >> $BASE_HOME/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
export HADOOP_SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10"
export PDSH_RCMD_TYPE=ssh
EOT
source $BASE_HOME/.bashrc

# --- /etc/hosts ---
echo "=== CONFIGURE /etc/hosts ==="
sudo bash -c "cat >> /etc/hosts" <<EOT
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
EOT

# --- SSH SETUP ---
echo "=== SSH KEYS & ACCESS ==="
ssh-keygen -t rsa -N "" -f $BASE_HOME/.ssh/id_rsa
ssh-copy-id $DN_USER@$DN1_PRIVATE_IP
ssh-copy-id $DN_USER@$DN2_PRIVATE_IP
ssh-copy-id localhost

# --- DISTRIBUTE HADOOP & SPARK ---
echo "=== DISTRIBUTE HADOOP & SPARK TO DATANODES ==="
scp -r $HADOOP_HOME $DN_USER@$DN1_PRIVATE_IP:$BASE_HOME/
scp -r $HADOOP_HOME $DN_USER@$DN2_PRIVATE_IP:$BASE_HOME/
scp -r $SPARK_HOME $DN_USER@$DN1_PRIVATE_IP:/opt/
scp -r $SPARK_HOME $DN_USER@$DN2_PRIVATE_IP:/opt/

# --- REMOTE ENV SETUP ON DATANODES ---
echo "=== SETUP ENVIRONMENT ON DATANODES ==="
for node in $DN1_PRIVATE_IP $DN2_PRIVATE_IP; do
ssh $DN_USER@$node <<EOF
cat <<EOT >> ~/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$BASE_HOME/hadoop
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOT
source ~/.bashrc
EOF
done

# --- CREATE DATA DIRS ---
echo "=== CREATE HADOOP DATA DIRECTORIES ==="
mkdir -p $HADOOP_HOME/data/namenode $HADOOP_HOME/data/datanode
for node in $DN1_PRIVATE_IP $DN2_PRIVATE_IP; do
ssh $DN_USER@$node "mkdir -p $BASE_HOME/hadoop/data/namenode $BASE_HOME/hadoop/data/datanode"
done

# --- HADOOP CONFIG ---
echo "=== HADOOP CONFIGURATION ==="
cat <<EOT > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$NN_PRIVATE_IP:9000</value>
  </property>
</configuration>
EOT

cat <<EOT > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://$HADOOP_HOME/data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://$HADOOP_HOME/data/datanode</value>
  </property>
</configuration>
EOT

cat <<EOT > $HADOOP_HOME/etc/hadoop/workers
datanode1
datanode2
EOT

# --- YARN CONFIG ---
cat <<EOT > $HADOOP_HOME/etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOT

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

# --- SYNC HADOOP CONFIG TO DATANODES ---
echo "=== SYNC CONFIG TO DATANODES ==="
for node in $DN1_PRIVATE_IP $DN2_PRIVATE_IP; do
scp $HADOOP_HOME/etc/hadoop/*.xml $DN_USER@$node:$BASE_HOME/hadoop/etc/hadoop/
scp $HADOOP_HOME/etc/hadoop/workers $DN_USER@$node:$BASE_HOME/hadoop/etc/hadoop/
done

# --- SPARK CONFIG ---
echo "=== CONFIGURE SPARK ==="
cat <<EOT > $SPARK_HOME/conf/spark-defaults.conf
spark.master yarn
spark.submit.deploy-mode client
EOT

for node in $DN1_PRIVATE_IP $DN2_PRIVATE_IP; do
scp $SPARK_HOME/conf/spark-defaults.conf $DN_USER@$node:/opt/spark/conf/
done

# --- MINIO INSTALL ---
echo "=== INSTALL MINIO ==="
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/
mkdir -p ~/minio-data

# --- AIRFLOW INSTALL ---
echo "=== INSTALL AIRFLOW ==="
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install --upgrade pip
pip install "apache-airflow==2.9.0" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt"

echo "=== SETUP COMPLETE ==="
echo "You can now start HDFS, YARN, Spark, Airflow, MinIO, and upload your jobs/data."