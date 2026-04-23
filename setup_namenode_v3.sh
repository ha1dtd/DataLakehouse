#!/bin/bash
set -euo pipefail

echo "=== NAMENODE LAKEHOUSE SERVICES SETUP (TRINO + SPARK THRIFT + SUPERSET) ==="

NN_USER=$(whoami)
BASE_HOME="/home/$NN_USER"
HADOOP_HOME="$BASE_HOME/hadoop"
SPARK_HOME="/opt/spark"
TRINO_HOME="$BASE_HOME/trino"
TRINO_CLI="$BASE_HOME/trino-cli"
SUPERSET_VENV="$BASE_HOME/superset-venv"
SUPERSET_CONFIG="$BASE_HOME/superset_config.py"

JAVA11_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"
JAVA17_HOME="/usr/lib/jvm/temurin-17-jdk-amd64"

TRINO_VERSION="435"
ICEBERG_VERSION="1.4.3"
HADOOP_AWS_VERSION="3.3.4"
AWS_BUNDLE_VERSION="1.12.262"

read -r -p "MinIO endpoint host:port [192.168.101.66:9001]: " MINIO_ENDPOINT
MINIO_ENDPOINT=${MINIO_ENDPOINT:-192.168.101.66:9001}
read -r -p "MinIO access key [admin]: " MINIO_ACCESS_KEY
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-admin}
read -r -p "MinIO secret key [12345678]: " MINIO_SECRET_KEY
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-12345678}
read -r -p "Airflow DAG file path [$BASE_HOME/airflow/dags/test_dag.py]: " DAG_FILE
DAG_FILE=${DAG_FILE:-$BASE_HOME/airflow/dags/test_dag.py}
read -r -p "Silver warehouse prefix [lakehouse]: " WAREHOUSE_PREFIX
WAREHOUSE_PREFIX=${WAREHOUSE_PREFIX:-lakehouse}
read -r -p "Superset admin username [admin]: " SUPERSET_ADMIN_USER
SUPERSET_ADMIN_USER=${SUPERSET_ADMIN_USER:-admin}
read -r -p "Superset admin password [admin]: " SUPERSET_ADMIN_PASS
SUPERSET_ADMIN_PASS=${SUPERSET_ADMIN_PASS:-admin}

echo "===================="
echo "STEP: INSTALL BASE PACKAGES"
echo "===================="
BASE_PKGS="wget curl tar unzip python3 python3-venv python3-pip build-essential libssl-dev libffi-dev python3-dev libsasl2-dev libldap2-dev default-libmysqlclient-dev"
MISSING_PKGS=""
for p in $BASE_PKGS; do
  if dpkg -s "$p" >/dev/null 2>&1; then
    :
  else
    MISSING_PKGS="$MISSING_PKGS $p"
  fi
done
if [ -n "$MISSING_PKGS" ]; then
  sudo apt update
  sudo apt install -y $MISSING_PKGS
  echo "  - Installed missing packages:$MISSING_PKGS"
else
  echo "  - Already installed"
fi

if [ ! -x /usr/bin/python ]; then
  sudo ln -sf /usr/bin/python3 /usr/bin/python
  echo "  - Linked /usr/bin/python -> /usr/bin/python3"
else
  echo "  - /usr/bin/python already exists"
fi

echo "===================="
echo "STEP: INSTALL JAVA 17"
echo "===================="
if [ ! -f /usr/share/keyrings/adoptium.gpg ]; then
  wget -4 -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg
fi
if [ ! -f /etc/apt/sources.list.d/adoptium.list ]; then
  . /etc/os-release
  ADOPT_CODENAME="${VERSION_CODENAME:-bookworm}"
  echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb ${ADOPT_CODENAME} main" | sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
fi

if [ -d "$JAVA17_HOME" ]; then
  echo "  - Java 17 already installed"
else
  sudo apt update
  sudo apt install -y temurin-17-jdk
  echo "  - Java 17 installed"
fi

echo "===================="
echo "STEP: MAKE HADOOP JAVA_HOME FLEXIBLE"
echo "===================="
HADOOP_ENV="$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
if [ -f "$HADOOP_ENV" ]; then
  sed -i 's|^export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/temurin-11-jdk-amd64}|' "$HADOOP_ENV"
  echo "  - Updated $HADOOP_ENV"
else
  echo "  - SKIP: $HADOOP_ENV not found"
fi

echo "===================="
echo "STEP: INSTALL TRINO SERVER + CLI"
echo "===================="
if [ -x "$TRINO_HOME/bin/launcher" ]; then
  echo "  - Trino server already installed"
else
  cd "$BASE_HOME"
  TRINO_ARCHIVE="trino-server-${TRINO_VERSION}.tar.gz"
  TRINO_URL="https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/${TRINO_ARCHIVE}"
  wget -4 -O "$TRINO_ARCHIVE" "$TRINO_URL"
  tar -xzf "$TRINO_ARCHIVE"
  mv "trino-server-${TRINO_VERSION}" "$TRINO_HOME"
  echo "  - Trino server installed at $TRINO_HOME"
fi

if [ -x "$TRINO_CLI" ]; then
  echo "  - Trino CLI already installed"
else
  cd "$BASE_HOME"
  TRINO_CLI_URL="https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"
  wget -4 -O "$TRINO_CLI" "$TRINO_CLI_URL"
  chmod +x "$TRINO_CLI"
  echo "  - Trino CLI installed at $TRINO_CLI"
fi

echo "===================="
echo "STEP: CONFIGURE TRINO"
echo "===================="
mkdir -p "$TRINO_HOME/etc/catalog" "$TRINO_HOME/data"

if [ ! -f "$TRINO_HOME/etc/node.properties" ]; then
  NODE_ID="$(cat /proc/sys/kernel/random/uuid)"
  cat <<EOT > "$TRINO_HOME/etc/node.properties"
node.environment=production
node.id=$NODE_ID
node.data-dir=$TRINO_HOME/data
EOT
  echo "  - node.properties created"
else
  echo "  - node.properties already exists"
fi

cat <<EOT > "$TRINO_HOME/etc/config.properties"
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8083
discovery.uri=http://localhost:8083
EOT
echo "  - config.properties written"

cat <<EOT > "$TRINO_HOME/etc/catalog/gold.properties"
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://namenode:9083
fs.native-s3.enabled=true
s3.endpoint=http://$MINIO_ENDPOINT
s3.aws-access-key=$MINIO_ACCESS_KEY
s3.aws-secret-key=$MINIO_SECRET_KEY
s3.path-style-access=true
s3.region=us-east-1
EOT

cat <<EOT > "$TRINO_HOME/etc/catalog/silver.properties"
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://namenode:9083
fs.native-s3.enabled=true
s3.endpoint=http://$MINIO_ENDPOINT
s3.aws-access-key=$MINIO_ACCESS_KEY
s3.aws-secret-key=$MINIO_SECRET_KEY
s3.path-style-access=true
s3.region=us-east-1
EOT

echo "  - Trino catalog configs written"

echo "===================="
echo "STEP: START / RESTART TRINO"
echo "===================="
if "$TRINO_HOME/bin/launcher" status >/dev/null 2>&1; then
  "$TRINO_HOME/bin/launcher" stop || true
  echo "  - Existing Trino stopped"
else
  echo "  - Trino not running, starting fresh"
fi
JAVA_HOME="$JAVA17_HOME" "$TRINO_HOME/bin/launcher" start
sleep 8
if "$TRINO_HOME/bin/launcher" status >/dev/null 2>&1; then
  echo "  - Trino started"
else
  echo "  - Trino failed to start"
  exit 1
fi

echo "===================="
echo "STEP: PATCH AIRFLOW DAG FOR LAKEHOUSE PREFIX"
echo "===================="
if [ -f "$DAG_FILE" ]; then
  python3 - <<PY
from pathlib import Path
path = Path(r'''$DAG_FILE''')
s = path.read_text()
s = s.replace('s3a://silver/', 's3a://silver/$WAREHOUSE_PREFIX/')
s = s.replace('s3a://gold/', 's3a://gold/$WAREHOUSE_PREFIX/')
s = s.replace('s3a://silver/$WAREHOUSE_PREFIX/$WAREHOUSE_PREFIX/', 's3a://silver/$WAREHOUSE_PREFIX/')
s = s.replace('s3a://gold/$WAREHOUSE_PREFIX/$WAREHOUSE_PREFIX/', 's3a://gold/$WAREHOUSE_PREFIX/')
path.write_text(s)
print('patched')
PY
  echo "  - DAG patched to use s3a://silver/$WAREHOUSE_PREFIX/ and s3a://gold/$WAREHOUSE_PREFIX/"
else
  echo "  - SKIP: DAG file not found at $DAG_FILE"
fi

echo "===================="
echo "STEP: FIND RUNTIME JARS FOR SPARK THRIFT"
echo "===================="
ICEBERG_JAR_PATH=$(find "$BASE_HOME/.ivy2" -type f -name "*iceberg-spark-runtime-3.5_2.12*${ICEBERG_VERSION}*.jar" 2>/dev/null | sort | tail -1 || true)
HADOOP_AWS_JAR=$(find "$BASE_HOME/.ivy2" "$HADOOP_HOME" /opt/spark -type f -name "hadoop-aws-${HADOOP_AWS_VERSION}.jar" 2>/dev/null | sort | tail -1 || true)
AWS_BUNDLE_JAR=$(find "$BASE_HOME/.ivy2" "$HADOOP_HOME" /opt/spark -type f -name "aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" 2>/dev/null | sort | tail -1 || true)

if [ -n "$ICEBERG_JAR_PATH" ]; then
  echo "  - Iceberg runtime jar found: $ICEBERG_JAR_PATH"
else
  echo "  - Iceberg runtime jar not found yet; Spark Thrift will still use --packages"
fi
if [ -n "$HADOOP_AWS_JAR" ]; then
  echo "  - Hadoop AWS jar found: $HADOOP_AWS_JAR"
else
  echo "  - Hadoop AWS jar not found yet; Spark Thrift will still use --packages"
fi
if [ -n "$AWS_BUNDLE_JAR" ]; then
  echo "  - AWS bundle jar found: $AWS_BUNDLE_JAR"
else
  echo "  - AWS bundle jar not found yet; Spark Thrift will still use --packages"
fi

EXTRA_CLASSPATH=""
[ -n "$ICEBERG_JAR_PATH" ] && EXTRA_CLASSPATH="$ICEBERG_JAR_PATH"
[ -n "$HADOOP_AWS_JAR" ] && EXTRA_CLASSPATH="${EXTRA_CLASSPATH:+$EXTRA_CLASSPATH:}$HADOOP_AWS_JAR"
[ -n "$AWS_BUNDLE_JAR" ] && EXTRA_CLASSPATH="${EXTRA_CLASSPATH:+$EXTRA_CLASSPATH:}$AWS_BUNDLE_JAR"

echo "===================="
echo "STEP: START / RESTART SPARK THRIFT SERVER"
echo "===================="
if pgrep -f 'HiveThriftServer2' >/dev/null 2>&1; then
  "$SPARK_HOME/sbin/stop-thriftserver.sh" || true
  echo "  - Existing Spark Thrift Server stopped"
else
  echo "  - Spark Thrift Server not running, starting fresh"
fi

THRIFT_CMD=(
  "$SPARK_HOME/sbin/start-thriftserver.sh"
  --master yarn
  --deploy-mode client
  --driver-memory 2G
  --executor-memory 2G
  --executor-cores 1
  --conf spark.executor.instances=1
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},com.amazonaws:aws-java-sdk-bundle:${AWS_BUNDLE_VERSION}"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.silver_catalog.type=hadoop
  --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/$WAREHOUSE_PREFIX/
  --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.gold_catalog.type=hadoop
  --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/$WAREHOUSE_PREFIX/
  --conf spark.hadoop.fs.s3a.endpoint=http://$MINIO_ENDPOINT
  --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY
  --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
)

if [ -n "$EXTRA_CLASSPATH" ]; then
  THRIFT_CMD+=(--conf "spark.driver.extraClassPath=$EXTRA_CLASSPATH")
  THRIFT_CMD+=(--conf "spark.executor.extraClassPath=$EXTRA_CLASSPATH")
  echo "  - Using extra classpath jars for Spark Thrift"
else
  echo "  - No extra classpath jars added"
fi

"${THRIFT_CMD[@]}"
sleep 8
if ss -tuln | grep -q ':10000 '; then
  echo "  - Spark Thrift Server started on 10000"
else
  echo "  - Spark Thrift Server failed to start on 10000"
  exit 1
fi

echo "===================="
echo "STEP: INSTALL + INIT SUPERSET"
echo "===================="
if [ -d "$SUPERSET_VENV" ]; then
  echo "  - Superset virtualenv already exists"
else
  python3 -m venv "$SUPERSET_VENV"
  echo "  - Superset virtualenv created"
fi

# shellcheck source=/dev/null
source "$SUPERSET_VENV/bin/activate"

if command -v superset >/dev/null 2>&1; then
  echo "  - Superset already installed"
else
  pip install --upgrade pip setuptools wheel
  pip install apache-superset pyhive thrift sasl thrift-sasl pure-sasl
  echo "  - Superset installed"
fi

if [ -f "$SUPERSET_CONFIG" ] && grep -q 'SECRET_KEY' "$SUPERSET_CONFIG"; then
  echo "  - Superset config already exists"
else
  SECRET_VALUE=$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(42))
PY
)
  cat <<EOT > "$SUPERSET_CONFIG"
SECRET_KEY = '$SECRET_VALUE'
EOT
  echo "  - Superset config created at $SUPERSET_CONFIG"
fi

export SUPERSET_CONFIG_PATH="$SUPERSET_CONFIG"
export FLASK_APP=superset
superset db upgrade

if superset fab list-users 2>/dev/null | grep -q "^$SUPERSET_ADMIN_USER "; then
  echo "  - Superset admin user already exists"
else
  superset fab create-admin \
    --username "$SUPERSET_ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password "$SUPERSET_ADMIN_PASS"
  echo "  - Superset admin user created"
fi

superset init
echo "  - Superset initialized"

deactivate

echo "=== SERVICES SETUP DONE ==="
echo ""
echo "Start commands:"
echo "  Trino:          JAVA_HOME=$JAVA17_HOME $TRINO_HOME/bin/launcher start"
echo "  Spark Thrift:   $SPARK_HOME/sbin/start-thriftserver.sh ... --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/$WAREHOUSE_PREFIX/ --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/$WAREHOUSE_PREFIX/"
echo "  Superset:       export SUPERSET_CONFIG_PATH=$SUPERSET_CONFIG && source $SUPERSET_VENV/bin/activate && superset run -p 8084 --with-threads --host 0.0.0.0"
echo ""
echo "Verify:"
echo "  Trino catalogs: JAVA_HOME=$JAVA17_HOME $TRINO_CLI --server localhost:8083 --execute \"SHOW CATALOGS;\""
echo "  Thrift port:    ss -tuln | grep 10000"
echo "  Superset port:  ss -tuln | grep 8084"
