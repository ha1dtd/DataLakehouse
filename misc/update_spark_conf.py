import yaml

config_file = "/home/daihai/spark_config.yaml"
spark_env_file = "/opt/spark/conf/spark-env.sh"

with open(config_file) as f:
    config = yaml.safe_load(f)

instances = config["spark_worker"]["instances"]
worker_cores = config["spark_worker"]["cores"]
executor_cores = config["spark_executor"]["cores"]

with open(spark_env_file, "w") as f:
    f.write("#!/usr/bin/env bash\n")
    f.write(f"SPARK_WORKER_INSTANCES={instances}\n")
    f.write(f"SPARK_WORKER_CORES={worker_cores}\n")
    f.write(f"SPARK_EXECUTOR_CORES={executor_cores}\n")

print("Spark configuration updated from YAML.")