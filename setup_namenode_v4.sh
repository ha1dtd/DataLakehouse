#!/bin/bash
set -euo pipefail

echo "=== NAMENODE FULL SETUP V4 (CORE CLUSTER + TRINO + SPARK THRIFT + SUPERSET) ==="

read -r -p "Namenode PRIVATE IP: " NN_PRIVATE_IP
read -r -p "Datanode1 PRIVATE IP: " DN1_PRIVATE_IP
read -r -p "Datanode2 PRIVATE IP: " DN2_PRIVATE_IP
read -r -p "Datanode3 PRIVATE IP: " DN3_PRIVATE_IP
read -r -p "Datanode4 PRIVATE IP: " DN4_PRIVATE_IP
read -r -p "Datanode5 PRIVATE IP: " DN5_PRIVATE_IP
read -r -p "Datanode username: " DN_USER
read -r -p "MinIO endpoint host:port [192.168.100.66:9001]: " MINIO_ENDPOINT
MINIO_ENDPOINT=${MINIO_ENDPOINT:-192.168.100.66:9001}
read -r -p "MinIO access key [admin]: " MINIO_ACCESS_KEY
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-admin}
read -r -p "MinIO secret key [12345678]: " MINIO_SECRET_KEY
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-12345678}
read -r -p "Airflow DAG file path [/home/$(whoami)/airflow/dags/test_dag.py]: " DAG_FILE

NN_USER=$(whoami)
BASE_HOME="/home/$NN_USER"
DAG_FILE=${DAG_FILE:-$BASE_HOME/airflow/dags/test_dag.py}
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
WAREHOUSE_PREFIX="lakehouse"
SUPERSET_ADMIN_USER="admin"
SUPERSET_ADMIN_PASS="admin"

echo "===================="
echo "STEP: CONFIGURE SSH KEY ACCESS"
echo "===================="
if [ -f ~/.ssh/id_rsa ]; then
  echo "  - SSH key already exists"
else
  ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
  echo "  - SSH key generated"
fi
mkdir -p ~/.ssh
if grep -qF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys 2>/dev/null; then
  echo "  - Local authorized_keys already configured"
else
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  echo "  - Local authorized_keys updated"
fi
chmod 600 ~/.ssh/authorized_keys
ssh-copy-id "$DN_USER@$DN1_PRIVATE_IP"
ssh-copy-id "$DN_USER@$DN2_PRIVATE_IP"
ssh-copy-id "$DN_USER@$DN3_PRIVATE_IP"
ssh-copy-id "$DN_USER@$DN4_PRIVATE_IP"
ssh-copy-id "$DN_USER@$DN5_PRIVATE_IP"

echo "===================="
echo "STEP: CONFIGURE NOPASSWD FOR DATANODE PUSH"
echo "===================="
for DN in $DN1_PRIVATE_IP $DN2_PRIVATE_IP $DN3_PRIVATE_IP $DN4_PRIVATE_IP $DN5_PRIVATE_IP; do
  echo "  - Checking sudoers on Datanode $DN"
  if ssh "$DN_USER@$DN" "sudo -n true" 2>/dev/null; then
    echo "    * NOPASSWD already configured on $DN"
  else
    echo "    * Configuring NOPASSWD on $DN (you will be prompted for $DN_USER's password once)"
    ssh -t "$DN_USER@$DN" "echo '$DN_USER ALL=(ALL) NOPASSWD:ALL' | sudo tee /etc/sudoers.d/$DN_USER >/dev/null"
    echo "    * NOPASSWD configured on $DN"
  fi
done

echo "===================="
echo "STEP: CONFIGURE APT MIRROR (KAKAO)"
echo "===================="
if grep -qE "mirror\\.kakao\\.com/ubuntu" /etc/apt/sources.list; then
  echo "  - Already configured"
else
  sudo sed -i 's|http://archive.ubuntu.com/ubuntu|http://mirror.kakao.com/ubuntu|g' /etc/apt/sources.list
  sudo sed -i 's|http://security.ubuntu.com/ubuntu|http://mirror.kakao.com/ubuntu|g' /etc/apt/sources.list
  echo "  - Configured"
fi

echo "===================="
echo "STEP: INSTALL BASE SYSTEM PACKAGES"
echo "===================="
BASE_PKGS="wget gpg ssh pdsh python3-venv python3-pip curl tar rsync unzip build-essential libssl-dev libffi-dev python3-dev libsasl2-dev libldap2-dev default-libmysqlclient-dev"
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
echo "STEP: INSTALL JAVA 11 (TEMURIN 11)"
echo "===================="
if [ -d "$JAVA11_HOME" ]; then
  echo "  - Already installed"
else
  if [ ! -f /usr/share/keyrings/adoptium.gpg ]; then
    wget -4 -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg
  fi
  . /etc/os-release
  ADOPT_CODENAME="${VERSION_CODENAME:-bookworm}"
  if [ ! -f /etc/apt/sources.list.d/adoptium.list ]; then
    echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb ${ADOPT_CODENAME} main" | sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
  fi
  sudo apt update
  sudo apt install -y temurin-11-jdk
  echo "  - Java 11 installed"
fi

[ -d "$JAVA11_HOME" ] || { echo "JAVA_HOME invalid: $JAVA11_HOME"; exit 1; }

echo "===================="
echo "STEP: INSTALL JAVA 17"
echo "===================="
if [ -d "$JAVA17_HOME" ]; then
  echo "  - Already installed"
else
  sudo apt update
  sudo apt install -y temurin-17-jdk
  echo "  - Java 17 installed"
fi

echo "===================="
echo "STEP: INSTALL HADOOP"
echo "===================="
if [ -x "$HADOOP_HOME/bin/hdfs" ]; then
  echo "  - Already installed"
else
  cd "$BASE_HOME"
  wget -4 https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
  tar -xzf hadoop-3.3.6.tar.gz
  mv hadoop-3.3.6 "$HADOOP_HOME"
  echo "  - Installed"
fi

echo "===================="
echo "STEP: INSTALL SPARK"
echo "===================="
if [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "  - Already installed"
else
  cd "$BASE_HOME"
  wget -4 https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
  tar -xzf spark-3.5.8-bin-hadoop3.tgz
  sudo mv spark-3.5.8-bin-hadoop3 "$SPARK_HOME"
  sudo chown -R "$NN_USER:$NN_USER" "$SPARK_HOME"
  echo "  - Installed"
fi

echo "===================="
echo "STEP: CONFIGURE SHELL ENVIRONMENT"
echo "===================="
if grep -qE "HADOOP_HOME" ~/.bashrc; then
  echo "  - Already configured"
else
  cat <<EOT >> ~/.bashrc
export JAVA_HOME=$JAVA11_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
export HADOOP_SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10"
export PDSH_RCMD_TYPE=ssh
EOT
  echo "  - Configured"
fi
export JAVA_HOME="$JAVA11_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export SPARK_HOME="$SPARK_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA11_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
echo "  - Environment exported for current script run"

echo "===================="
echo "STEP: CONFIGURE /etc/hosts"
echo "===================="
HOSTS_BEGIN="# >>> FOXAI CLUSTER HOSTS >>>"
HOSTS_END="# <<< FOXAI CLUSTER HOSTS <<<"
EXPECTED_BLOCK="$HOSTS_BEGIN
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
$DN3_PRIVATE_IP datanode3
$DN4_PRIVATE_IP datanode4
$DN5_PRIVATE_IP datanode5
$HOSTS_END"
if sudo grep -Fqx "$HOSTS_BEGIN" /etc/hosts 2>/dev/null; then
  CURRENT_BLOCK=$(sudo awk "/^${HOSTS_BEGIN//\//\\/}
$/,/^${HOSTS_END//\//\\/}
$/" /etc/hosts)
  if [ "$CURRENT_BLOCK" = "$EXPECTED_BLOCK" ]; then
    echo "  - Already configured (managed hosts block matches expected IPs)"
  else
    sudo python3 - <<PY
from pathlib import Path
path = Path('/etc/hosts')
text = path.read_text()
begin = '# >>> FOXAI CLUSTER HOSTS >>>\n'
end = '# <<< FOXAI CLUSTER HOSTS <<<\n'
start = text.find(begin)
if start != -1:
    finish = text.find(end, start)
    if finish != -1:
        finish += len(end)
        text = text[:start] + text[finish:]
block = """# >>> FOXAI CLUSTER HOSTS >>>
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
$DN3_PRIVATE_IP datanode3
$DN4_PRIVATE_IP datanode4
$DN5_PRIVATE_IP datanode5
# <<< FOXAI CLUSTER HOSTS <<<
"""
if text and not text.endswith('\n'):
    text += '\n'
text += block
path.write_text(text)
PY
    echo "  - Updated managed hosts block with expected IPs"
  fi
else
  sudo python3 - <<PY
from pathlib import Path
path = Path('/etc/hosts')
text = path.read_text()
block = """# >>> FOXAI CLUSTER HOSTS >>>
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
$DN3_PRIVATE_IP datanode3
$DN4_PRIVATE_IP datanode4
$DN5_PRIVATE_IP datanode5
# <<< FOXAI CLUSTER HOSTS <<<
"""
if text and not text.endswith('\n'):
    text += '\n'
text += block
path.write_text(text)
PY
  echo "  - Added managed hosts block"
fi

echo "===================="
echo "STEP: CREATE HADOOP DATA DIRECTORIES"
echo "===================="
if [ -d "$BASE_HOME/hadoopdata/namenode" ]; then
  chmod -R 700 "$BASE_HOME/hadoopdata"
  echo "  - Already exists (permissions refreshed)"
else
  mkdir -p "$BASE_HOME/hadoopdata/namenode"
  chmod -R 700 "$BASE_HOME/hadoopdata"
  echo "  - Created"
fi

echo "===================="
echo "STEP: CONFIGURE HADOOP AND YARN XML"
echo "===================="
if [ -f "$HADOOP_HOME/etc/hadoop/core-site.xml" ] && grep -qE "hdfs://namenode:9000" "$HADOOP_HOME/etc/hadoop/core-site.xml"; then
  echo "  - core-site.xml already configured"
else
cat <<EOT > "$HADOOP_HOME/etc/hadoop/core-site.xml"
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
</configuration>
EOT
  echo "  - core-site.xml configured"
fi

if [ -f "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" ] && grep -qE "dfs\\.datanode\\.data\\.dir" "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"; then
  echo "  - hdfs-site.xml already configured"
else
cat <<EOT > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml"
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$BASE_HOME/hadoopdata/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///home/$DN_USER/hadoopdata/datanode</value></property>
</configuration>
EOT
  echo "  - hdfs-site.xml configured"
fi

if [ -f "$HADOOP_HOME/etc/hadoop/workers" ] \
  && grep -qEx "datanode1" "$HADOOP_HOME/etc/hadoop/workers" \
  && grep -qEx "datanode5" "$HADOOP_HOME/etc/hadoop/workers"; then
  echo "  - workers already configured"
else
cat <<EOT > "$HADOOP_HOME/etc/hadoop/workers"
datanode1
datanode2
datanode3
datanode4
datanode5
EOT
  echo "  - workers configured"
fi

if [ -f "$HADOOP_HOME/etc/hadoop/mapred-site.xml" ] && grep -qE "mapreduce\\.framework\\.name" "$HADOOP_HOME/etc/hadoop/mapred-site.xml"; then
  echo "  - mapred-site.xml already configured"
else
cat <<'EOT' > "$HADOOP_HOME/etc/hadoop/mapred-site.xml"
<configuration>
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
</configuration>
EOT
  echo "  - mapred-site.xml configured"
fi

if [ -f "$HADOOP_HOME/etc/hadoop/yarn-site.xml" ] && grep -qE "yarn\\.resourcemanager\\.hostname" "$HADOOP_HOME/etc/hadoop/yarn-site.xml"; then
  echo "  - yarn-site.xml already configured"
else
cat <<'EOT' > "$HADOOP_HOME/etc/hadoop/yarn-site.xml"
<configuration>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>namenode</value>
</property>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.resource.memory-mb</name>
<value>14336</value>
</property>
<property>
<name>yarn.scheduler.maximum-allocation-mb</name>
<value>14336</value>
</property>
<property>
<name>yarn.nodemanager.resource.cpu-vcores</name>
<value>15</value>
</property>
</configuration>
EOT
  echo "  - yarn-site.xml configured"
fi

echo "===================="
echo "STEP: CONFIGURE JAVA_HOME IN hadoop-env.sh"
echo "===================="
HADOOP_ENV="$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
FLEXIBLE_JAVA="export JAVA_HOME=\${JAVA_HOME:-$JAVA11_HOME}"
if grep -qE "^export JAVA_HOME=\\$\\{JAVA_HOME" "$HADOOP_ENV"; then
  echo "  - Already configured (flexible JAVA_HOME already set)"
elif grep -qE '^# export JAVA_HOME=' "$HADOOP_ENV"; then
  sed -i "s|^# export JAVA_HOME=.*|$FLEXIBLE_JAVA|" "$HADOOP_ENV"
  echo "  - Updated from commented line"
elif grep -qE '^export JAVA_HOME=' "$HADOOP_ENV"; then
  sed -i "s|^export JAVA_HOME=.*|$FLEXIBLE_JAVA|" "$HADOOP_ENV"
  echo "  - Updated existing export JAVA_HOME value"
else
  echo "$FLEXIBLE_JAVA" >> "$HADOOP_ENV"
  echo "  - Added new flexible JAVA_HOME line"
fi

echo "===================="
echo "STEP: FORMAT NAMENODE (1ST RUN ONLY)"
echo "===================="
if [ -d "$BASE_HOME/hadoopdata/namenode/current" ]; then
  echo "  - Already formatted"
else
  "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
  echo "  - Namenode formatted"
fi

echo "===================="
echo "STEP: SYNC HADOOP/SPARK TO DATANODES"
echo "===================="
for DN in datanode1 datanode2 datanode3 datanode4 datanode5; do
  echo "  - Syncing to $DN"
  rsync -az --delete "$HADOOP_HOME/" "$DN_USER@$DN:$HADOOP_HOME/"
  rsync -az --delete "$SPARK_HOME/" "$DN_USER@$DN:$SPARK_HOME/"
done
echo "  - Hadoop and Spark synced"

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
s = s.replace('s3a://silver/', 's3a://silver/lakehouse/')
s = s.replace('s3a://gold/', 's3a://gold/lakehouse/')
s = s.replace('s3a://silver/lakehouse/lakehouse/', 's3a://silver/lakehouse/')
s = s.replace('s3a://gold/lakehouse/lakehouse/', 's3a://gold/lakehouse/')
path.write_text(s)
print('patched')
PY
  echo "  - DAG patched to use s3a://silver/lakehouse/ and s3a://gold/lakehouse/"
else
  echo "  - SKIP: DAG file not found at $DAG_FILE"
fi

echo "===================="
echo "STEP: FIND RUNTIME JARS FOR SPARK THRIFT"
echo "===================="
ICEBERG_JAR_PATH=$(find "$BASE_HOME/.ivy2" -type f -name "*iceberg-spark-runtime-3.5_2.12*${ICEBERG_VERSION}*.jar" 2>/dev/null | sort | tail -1 || true)
HADOOP_AWS_JAR=$(find "$BASE_HOME/.ivy2" "$HADOOP_HOME" /opt/spark -type f -name "hadoop-aws-${HADOOP_AWS_VERSION}.jar" 2>/dev/null | sort | tail -1 || true)
AWS_BUNDLE_JAR=$(find "$BASE_HOME/.ivy2" "$HADOOP_HOME" /opt/spark -type f -name "aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar" 2>/dev/null | sort | tail -1 || true)
[ -n "$ICEBERG_JAR_PATH" ] && echo "  - Iceberg runtime jar found: $ICEBERG_JAR_PATH" || echo "  - Iceberg runtime jar not found yet; Spark Thrift will still use --packages"
[ -n "$HADOOP_AWS_JAR" ] && echo "  - Hadoop AWS jar found: $HADOOP_AWS_JAR" || echo "  - Hadoop AWS jar not found yet; Spark Thrift will still use --packages"
[ -n "$AWS_BUNDLE_JAR" ] && echo "  - AWS bundle jar found: $AWS_BUNDLE_JAR" || echo "  - AWS bundle jar not found yet; Spark Thrift will still use --packages"
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
  --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/
  --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.gold_catalog.type=hadoop
  --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/
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
if superset fab list-users 2>/dev/null | grep -q '^admin '; then
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

echo "=== V4 SETUP DONE ==="
echo ""
echo "Start commands:"
echo "  Trino:          JAVA_HOME=$JAVA17_HOME $TRINO_HOME/bin/launcher start"
echo "  Spark Thrift:   $SPARK_HOME/sbin/start-thriftserver.sh ... --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/"
echo "  Superset:       export SUPERSET_CONFIG_PATH=$SUPERSET_CONFIG && source $SUPERSET_VENV/bin/activate && superset run -p 8084 --with-threads --host 0.0.0.0"
echo ""
echo "Verify:"
echo "  Trino catalogs: JAVA_HOME=$JAVA17_HOME $TRINO_CLI --server localhost:8083 --execute \"SHOW CATALOGS;\""
echo "  Thrift port:    ss -tuln | grep 10000"
echo "  Superset port:  ss -tuln | grep 8084"
