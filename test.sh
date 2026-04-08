#!/bin/bash

echo "=== FORMAT ==="
hdfs namenode -format

echo "=== START HADOOP ==="
start-dfs.sh
start-yarn.sh

echo "=== CHECK ==="
jps
hdfs dfsadmin -report

echo "=== TEST HDFS ==="
echo "hello" > test.txt
hdfs dfs -mkdir -p /test
hdfs dfs -put -f test.txt /test/

echo "=== START SPARK ==="
start-master.sh
start-workers.sh spark://localhost:7077

echo "=== START MINIO ==="
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin

nohup minio server ~/minio-data \
  --address ":9001" \
  --console-address ":9002" &

echo "=== START AIRFLOW ==="
source airflow-venv/bin/activate

nohup airflow webserver --port 8081 &
nohup airflow scheduler &

echo "=== DONE ==="