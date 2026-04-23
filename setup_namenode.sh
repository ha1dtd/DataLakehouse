#!/bin/bash
set -e

echo "=== NAMENODE SETUP ==="

read -r -p "Namenode PRIVATE IP: " NN_PRIVATE_IP
read -r -p "Datanode1 PRIVATE IP: " DN1_PRIVATE_IP
read -r -p "Datanode2 PRIVATE IP: " DN2_PRIVATE_IP
read -r -p "Datanode username: " DN_USER

NN_USER=$(whoami)
BASE_HOME="/home/$NN_USER"
HADOOP_HOME="$BASE_HOME/hadoop"
SPARK_HOME="/opt/spark"
JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"

echo "===================="
echo "STEP: CONFIGURE SSH KEY ACCESS"
echo "===================="
[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
mkdir -p ~/.ssh
if ! grep -qF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys 2>/dev/null; then
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
fi
chmod 600 ~/.ssh/authorized_keys
ssh-copy-id "$DN_USER@$DN1_PRIVATE_IP"
ssh-copy-id "$DN_USER@$DN2_PRIVATE_IP"

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
BASE_PKGS="wget gpg ssh pdsh python3-venv python3-pip curl tar rsync unzip"
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

echo "===================="
echo "STEP: INSTALL JAVA 11 (TEMURIN 11)"
echo "===================="
if [ -d "$JAVA_HOME" ]; then
  echo "  - Already installed"
else
  if [ ! -f /usr/share/keyrings/adoptium.gpg ]; then
    wget -4 -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg
  fi
  . /etc/os-release
  ADOPT_CODENAME="${VERSION_CODENAME:-bookworm}"
  echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb ${ADOPT_CODENAME} main" | sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
  sudo apt update
  sudo apt install -y temurin-11-jdk
fi
[ -d "$JAVA_HOME" ] || { echo "JAVA_HOME invalid: $JAVA_HOME"; exit 1; }

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
export JAVA_HOME=$JAVA_HOME
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
export JAVA_HOME="$JAVA_HOME"
export HADOOP_HOME="$HADOOP_HOME"
export SPARK_HOME="$SPARK_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
echo "  - Environment exported for current script run"

echo "===================="
echo "STEP: CONFIGURE /etc/hosts"
echo "===================="
if grep -qE "namenode" /etc/hosts; then
  echo "  - Already configured"
else
  sudo bash -c "cat >> /etc/hosts" <<EOT
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
EOT
  echo "  - Configured"
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

if [ -f "$HADOOP_HOME/etc/hadoop/workers" ] && grep -qE "datanode1" "$HADOOP_HOME/etc/hadoop/workers" && grep -qE "datanode2" "$HADOOP_HOME/etc/hadoop/workers"; then
  echo "  - workers already configured"
else
cat <<EOT > "$HADOOP_HOME/etc/hadoop/workers"
datanode1
datanode2
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
</configuration>
EOT
  echo "  - yarn-site.xml configured"
fi

echo "===================="
echo "STEP: CONFIGURE JAVA_HOME IN hadoop-env.sh"
echo "===================="
HADOOP_ENV="$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
FLEXIBLE_JAVA="export JAVA_HOME=\${JAVA_HOME:-$JAVA_HOME}"
if grep -qE "^export JAVA_HOME=\\\$\\{JAVA_HOME" "$HADOOP_ENV"; then
  echo "  - Already configured (flexible JAVA_HOME already set)"
elif grep -qE "^export JAVA_HOME=$JAVA_HOME" "$HADOOP_ENV"; then
  sed -i "s|^export JAVA_HOME=.*|$FLEXIBLE_JAVA|" "$HADOOP_ENV"
  echo "  - Updated to flexible JAVA_HOME"
elif grep -qE '^# export JAVA_HOME=' "$HADOOP_ENV"; then
  sed -i "s|^# export JAVA_HOME=.*|$FLEXIBLE_JAVA|" "$HADOOP_ENV"
  echo "  - Updated from commented line (# export JAVA_HOME=...)"
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
# Force the script to know where HADOOP_HOME is right now
# This ensures 'hdfs' can be found even if source ~/.bashrc failed
export HADOOP_HOME="$BASE_HOME/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

if [ -d "$BASE_HOME/hadoopdata/namenode/current" ]; then
  echo "  - Already formatted"
else
  # Using the absolute path is the safest way to prevent "command not found"
  $HADOOP_HOME/bin/hdfs namenode -format -nonInteractive
  echo "  - Formatted"
fi

echo "===================="
echo "STEP: PUSH HADOOP + SPARK TO DATANODES"
echo "===================="
for DN in $DN1_PRIVATE_IP $DN2_PRIVATE_IP; do
  echo "  - Datanode $DN"
  REMOTE_HADOOP="/home/$DN_USER/hadoop"
  REMOTE_SPARK="/opt/spark"

  HADOOP_OK=""
  SPARK_OK=""
  if ssh -o BatchMode=yes -o ConnectTimeout=10 "$DN_USER@$DN" "[ -x $REMOTE_HADOOP/bin/hdfs ]" 2>/dev/null; then
    HADOOP_OK=1
  fi
  if ssh -o BatchMode=yes -o ConnectTimeout=10 "$DN_USER@$DN" "[ -x $REMOTE_SPARK/bin/spark-submit ]" 2>/dev/null; then
    SPARK_OK=1
  fi

  if [ -n "$HADOOP_OK" ] && [ -n "$SPARK_OK" ]; then
    echo "    * Hadoop + Spark already present — skip push for this host"
    continue
  fi

  ssh "$DN_USER@$DN" "mkdir -p /home/$DN_USER/hadoop /home/$DN_USER/hadoopdata/datanode"

  if [ -z "$HADOOP_OK" ]; then
    if ssh "$DN_USER@$DN" "command -v rsync >/dev/null 2>&1"; then
      rsync -az --delete "$HADOOP_HOME/" "$DN_USER@$DN:/home/$DN_USER/hadoop/"
      echo "    * Hadoop synced via rsync"
    else
      echo "    * rsync missing on $DN, fallback to scp"
      ssh "$DN_USER@$DN" "rm -rf /home/$DN_USER/hadoop"
      scp -r "$HADOOP_HOME" "$DN_USER@$DN:/home/$DN_USER/"
      echo "    * Hadoop copied via scp"
    fi
  else
    echo "    * Hadoop already present — skip Hadoop copy"
  fi

  if [ -z "$SPARK_OK" ]; then
    # Permission-safe flow: copy Spark to home first, then sudo move to /opt.
    scp -r "$SPARK_HOME" "$DN_USER@$DN:/home/$DN_USER/spark_tmp"
    ssh "$DN_USER@$DN" "sudo mv /home/$DN_USER/spark_tmp /opt/spark && sudo chown -R $DN_USER:$DN_USER /opt/spark"
    echo "    * Spark pushed to $REMOTE_SPARK"
  else
    echo "    * Spark already present — skip Spark copy"
  fi
done

echo "===================="
echo "STEP: INSTALL AND SETUP MINIO (skip binary if present; skip start if process already running)"
echo "===================="
if command -v minio >/dev/null 2>&1; then
  echo "  - MinIO binary already installed"
else
  wget -4 https://dl.min.io/server/minio/release/linux-amd64/minio
  chmod +x minio
  sudo mv minio /usr/local/bin/
  echo "  - MinIO binary installed"
fi
mkdir -p "$BASE_HOME/minio-data"
if pgrep -f "minio server" >/dev/null 2>&1; then
  echo "  - MinIO already running"
else
  export MINIO_ROOT_USER=admin
  export MINIO_ROOT_PASSWORD=12345678
  nohup minio server "$BASE_HOME/minio-data" --address ":9001" --console-address ":9002" > "$BASE_HOME/minio.log" 2>&1 &
  echo "  - MinIO started"
fi

echo "===================="
echo "STEP: INSTALL AND INITIALIZE AIRFLOW (skip venv/pip/db/user when already done)"
echo "===================="
if [ -d "$BASE_HOME/airflow-venv" ]; then
  echo "  - Virtualenv already exists"
else
  python3 -m venv "$BASE_HOME/airflow-venv"
  echo "  - Virtualenv created"
fi
# shellcheck source=/dev/null
source "$BASE_HOME/airflow-venv/bin/activate"
if python -c "import airflow" >/dev/null 2>&1; then
  echo "  - Airflow already installed (skipped pip upgrade + install)"
else
  pip install --upgrade pip
  PY_MM="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-${PY_MM}.txt"
  if curl -fsSL -o /tmp/airflow-constraints.txt "$CONSTRAINT_URL"; then
    pip install "apache-airflow==2.9.0" --constraint /tmp/airflow-constraints.txt
  else
    pip install "apache-airflow==2.9.0"
  fi
  echo "  - Airflow installed"
fi
export AIRFLOW_HOME=$BASE_HOME/airflow
mkdir -p "$AIRFLOW_HOME"
if [ -f "$AIRFLOW_HOME/airflow.db" ]; then
  echo "  - Airflow DB already initialized"
else
  airflow db init
  echo "  - Airflow DB initialized"
fi
if airflow users list 2>/dev/null | grep -qE "admin@example.com"; then
  echo "  - Airflow admin user already exists"
else
  airflow users create     --username admin     --firstname admin     --lastname admin     --role Admin     --email admin@example.com     --password admin
  echo "  - Airflow admin user created"
fi
deactivate

echo "===================="
echo "STEP: FINAL CHECK (always runs jps / hdfs report — informational, not install)"
echo "===================="
jps || true
hdfs dfsadmin -report || true

echo "=== NAMENODE SETUP DONE ==="
echo "You can now start DFS: start-dfs.sh"
echo "And start YARN: start-yarn.sh"