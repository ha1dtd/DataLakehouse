# Superset + Spark Thrift Runbook

This is the step-by-step runtime sequence on `namenode` for bringing up the query stack used by Superset.

It is more complete than the old snippet because Spark Thrift is not enough by itself. For Superset queries to work reliably against Iceberg tables on MinIO, the following must already be healthy:

- HDFS
- YARN
- MinIO
- Spark Thrift Server with the same Iceberg/S3A package line used by the pipeline
- Superset

If Superset shows a generic Apache Hive error, the first suspect is usually Spark Thrift startup/config, especially missing or inconsistent Iceberg / `hadoop-aws` / AWS SDK jars.

Important access distinction:

- `localhost` inside shell commands means "from namenode itself"
- `localhost:8084` in your browser usually means you are reaching namenode through SSH / VSCode port forwarding from your local machine
- in your current setup, `beeline` works reliably with `localhost:10000`, while the Superset database URL can use `127.0.0.1:10000`

## 0. Important Notes

- Run these on `namenode`.
- Use the same versions already used by the pipeline:
  - `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3`
  - `org.apache.hadoop:hadoop-aws:3.3.4`
  - `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- Do not mix different `iceberg`, `hadoop-aws`, or AWS SDK versions between pipeline jobs and Spark Thrift.
- The old note only exposed `silver_catalog`. This runbook exposes `raw`, `bronze`, `silver`, and `gold` so Superset or Beeline can query the whole Medallion stack if needed.

## 1. Load Runtime Environment

```bash
cd /home/ubuntu
source ~/.bashrc

export JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
export HADOOP_HOME=/home/ubuntu/hadoop
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
export SUPERSET_CONFIG_PATH=~/superset_config.py
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

## 2. Stop Stale Processes First

```bash
pkill -f "superset run" || true
pkill -f "HiveThriftServer2|start-thriftserver.sh" || true
sleep 3
```

Optional quick check:

```bash
ss -ltnp | grep -E ':8084|:10000' || true
```

## 3. Start HDFS

```bash
start-dfs.sh
jps
```

Minimum local check on namenode:

- `NameNode`
- `SecondaryNameNode`

## 4. Start YARN

```bash
start-yarn.sh
jps
```

Minimum local check on namenode:

- `ResourceManager`
- `NodeManager` if it runs on namenode in your setup

Web UI:

```text
http://192.168.100.66:8088
```

If you are viewing it from your local machine through SSH / VSCode port forwarding, use your forwarded local port instead of the raw namenode IP.

## 5. Make Sure MinIO Is Running

Check first:

```bash
pgrep -af "minio server" || true
curl -I http://localhost:9001 || true
```

If MinIO is not running, start it:

```bash
nohup minio server ~/minio-data --address ":9001" --console-address ":9002" > ~/minio.log 2>&1 &
sleep 3
curl -I http://localhost:9001
```

## 6. Start Superset

```bash
source ~/superset-venv/bin/activate
nohup superset run -h 0.0.0.0 -p 8084 > /tmp/superset.out 2>&1 &
sleep 5
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8084/login
```

Expected:

- HTTP `200` on `/login`
- In your local browser, this is commonly opened as `http://localhost:8084` when port forwarding is active

## 7. Start Spark Thrift Server

This is the important part. Use the same Iceberg / S3A dependency line as the Spark pipeline.

```bash
nohup $SPARK_HOME/sbin/start-thriftserver.sh \
  --master yarn \
  --deploy-mode client \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  --conf spark.executor.instances=4 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.network.timeout=300s \
  --conf spark.sql.execution.arrow.enabled=false \
  --conf spark.sql.execution.arrow.pyspark.enabled=false \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=false \
  --conf spark.sql.files.maxPartitionBytes=134217728 \
  --conf spark.executor.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  --conf spark.driver.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.raw_catalog.type=hadoop \
  --conf spark.sql.catalog.raw_catalog.warehouse=s3a://raw/lakehouse/ \
  --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.bronze_catalog.type=hadoop \
  --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/lakehouse/ \
  --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver_catalog.type=hadoop \
  --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
  --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold_catalog.type=hadoop \
  --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://192.168.100.66:9001 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=12345678 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.authentication=NOSASL \
  > /tmp/spark-thrift.out 2>&1 &
```

Wait a bit, then verify:

```bash
sleep 15
ss -ltnp | grep ':10000'
tail -n 80 /tmp/spark-thrift.out
```

Expected:

- port `10000` is listening
- no obvious `ClassNotFoundException`, `NoSuchMethodError`, `No FileSystem for scheme s3a`, or Iceberg catalog startup errors in `/tmp/spark-thrift.out`

## 8. Validate Thrift Before Using Superset

Do this before opening Superset SQL Lab. If Beeline fails, Superset will fail too.

```bash
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW DATABASES;'
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN silver_catalog.default;'
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SELECT count(*) FROM silver_catalog.default.yellow_taxi;'
```

Optional wider checks:

```bash
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN raw_catalog.registry;'
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN bronze_catalog.control;'
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN gold_catalog.default;'
```

## 9. Connect Superset To Spark Thrift

Connection URL:

```text
hive://ubuntu@127.0.0.1:10000/default?auth=NOSASL
```

Why `127.0.0.1` here:

- this URL is used by the Superset process running on namenode
- Superset should connect locally to Spark Thrift on the same host
- in your current setup, this DB URL works in Superset even though manual `beeline` checks are more reliable with `localhost`
- this is independent from how you open the Superset web UI in your own browser

In Superset:

1. Open Superset in the browser.
   Typical operator path: `http://localhost:8084` if your SSH / VSCode port forwarding is active.
   Alternative direct path: `http://192.168.100.66:8084` only if the port is exposed and reachable directly.
2. Go to `Settings` -> `Database Connections`
3. Add a new database using the URL above
4. Run a simple validation query first:

```sql
SHOW TABLES IN silver_catalog.default;
```

## 10. Quick End-to-End Verify

If all of the following are true, the stack is ready:

- `curl http://localhost:8084/login` returns `200`
- `ss -ltnp | grep ':10000'` shows Spark Thrift listening
- `beeline` can query `silver_catalog.default`
- Superset can run a simple query without Apache Hive error

## 11. If Superset Shows Apache Hive Error

Check in this exact order:

1. `tail -n 120 /tmp/spark-thrift.out`
2. `ss -ltnp | grep ':10000'`
3. run the same query in `beeline`
4. confirm MinIO is reachable at `http://192.168.100.66:9001`
5. confirm the package line exactly matches the pipeline versions

The common root causes are:

- Spark Thrift started without the Iceberg jar
- Spark Thrift started without `hadoop-aws`
- Spark Thrift started with mismatched AWS SDK / Hadoop AWS versions
- missing `s3a` conf
- stale failed Thrift process still bound or half-started

It is usually not a Superset bug. Most of the time the failure is underneath in Spark Thrift startup or its jar/classpath state.

## 12. One-Block Restart Sequence

Use this when you just want the exact restart flow in order:

```bash
cd /home/ubuntu
source ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
export HADOOP_HOME=/home/ubuntu/hadoop
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
export SUPERSET_CONFIG_PATH=~/superset_config.py
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

pkill -f "superset run" || true
pkill -f "HiveThriftServer2|start-thriftserver.sh" || true
sleep 3

start-dfs.sh
start-yarn.sh

pgrep -af "minio server" || nohup minio server ~/minio-data --address ":9001" --console-address ":9002" > ~/minio.log 2>&1 &

source ~/superset-venv/bin/activate
nohup superset run -h 0.0.0.0 -p 8084 > /tmp/superset.out 2>&1 &

nohup $SPARK_HOME/sbin/start-thriftserver.sh \
  --master yarn \
  --deploy-mode client \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  --conf spark.executor.instances=4 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=64 \
  --conf spark.network.timeout=300s \
  --conf spark.sql.execution.arrow.enabled=false \
  --conf spark.sql.execution.arrow.pyspark.enabled=false \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=false \
  --conf spark.sql.files.maxPartitionBytes=134217728 \
  --conf spark.executor.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  --conf spark.driver.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.raw_catalog.type=hadoop \
  --conf spark.sql.catalog.raw_catalog.warehouse=s3a://raw/lakehouse/ \
  --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.bronze_catalog.type=hadoop \
  --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/lakehouse/ \
  --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver_catalog.type=hadoop \
  --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
  --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold_catalog.type=hadoop \
  --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://192.168.100.66:9001 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=12345678 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.authentication=NOSASL \
  > /tmp/spark-thrift.out 2>&1 &

sleep 15
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8084/login
ss -ltnp | grep ':10000'
/opt/spark/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN silver_catalog.default;'
```
