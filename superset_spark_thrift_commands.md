# Superset + Spark Thrift Commands (namenode local)

## 1) Kill Superset + Spark Thrift

```bash
pkill -f "superset" || true
pkill -f "HiveThriftServer2|start-thriftserver.sh" || true
```

## 2) Start Superset (port 8084)

```bash
source ~/superset-venv/bin/activate
export SUPERSET_CONFIG_PATH=~/superset_config.py
nohup superset run -h 0.0.0.0 -p 8084 > /tmp/superset.out 2>&1 &
```

## 3) Start Spark Thrift

```bash
export HADOOP_CONF_DIR=/home/ubuntu/hadoop/etc/hadoop
export YARN_CONF_DIR=/home/ubuntu/hadoop/etc/hadoop
export SPARK_DIST_CLASSPATH=$(/home/ubuntu/hadoop/bin/hadoop classpath)

cd /home/ubuntu
nohup /opt/spark/sbin/start-thriftserver.sh \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver_catalog.type=hadoop \
  --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
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

## 4) URL for database connection (Superset)

```text
hive://ubuntu@127.0.0.1:10000/default?auth=NOSASL
```

## 5) Query commands for silver catalog

```bash
/opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN silver_catalog.default;'
/opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;auth=noSasl' -n ubuntu -e 'SELECT count(*) FROM silver_catalog.default.yellow_taxi;'
/opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;auth=noSasl' -n ubuntu -e 'SELECT * FROM silver_catalog.default.yellow_taxi LIMIT 10;'
```

## Quick verify

```bash
ss -ltnp | grep -E ':8084|:10000'
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8084/login
```
