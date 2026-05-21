import logging
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from foxai_config import BRONZE_NAMESPACE, RAW_NAMESPACE, PG_SOURCE_TABLE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_sample_raw_to_bronze")


def main() -> None:
    spark = SparkSession.builder.appName("hdos_sample_raw_to_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_fqn = f"raw_catalog.{RAW_NAMESPACE}.{PG_SOURCE_TABLE}_raw"
    bronze_fqn = f"bronze_catalog.{BRONZE_NAMESPACE}.{PG_SOURCE_TABLE}_bronze"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bronze_catalog.{BRONZE_NAMESPACE}")

    df = spark.read.table(raw_fqn).select(
        col("nhanvienlogid").cast("int").alias("nhanvienlogid"),
        col("nhanvienid").cast("int").alias("nhanvienid"),
        col("logintime").cast("timestamp").alias("logintime"),
        col("dm_nhanvientypeid").cast("int").alias("dm_nhanvientypeid"),
        col("hardwareid").cast("string").alias("hardwareid"),
        col("processorid").cast("string").alias("processorid"),
        col("baseboardproduct").cast("string").alias("baseboardproduct"),
        col("baseboardmanufacturer").cast("string").alias("baseboardmanufacturer"),
        col("diskdrivesignature").cast("string").alias("diskdrivesignature"),
        col("videocontrollercaption").cast("string").alias("videocontrollercaption"),
        col("physicalmediaserialnumber").cast("string").alias("physicalmediaserialnumber"),
        col("biosversion").cast("string").alias("biosversion"),
        col("operatingsystemserialnumber").cast("string").alias("operatingsystemserialnumber"),
        col("ipaddress").cast("string").alias("ipaddress"),
        col("internetip").cast("string").alias("internetip"),
        col("macadddress").cast("string").alias("macadddress"),
        col("computername").cast("string").alias("computername"),
        col("username").cast("string").alias("username"),
        col("domain").cast("string").alias("domain"),
        col("softversion").cast("string").alias("softversion"),
        col("softfolder").cast("string").alias("softfolder"),
        col("version").cast("timestamp").alias("version"),
        col("_source_schema"),
        col("_source_table"),
        col("_ingested_at"),
    )

    logger.info("BRONZE_SOURCE=%s", raw_fqn)
    logger.info("BRONZE_TARGET=%s", bronze_fqn)
    logger.info("BRONZE_ROW_COUNT=%s", df.count())

    df.writeTo(bronze_fqn).createOrReplace()
    logger.info("BRONZE_WRITE_COMPLETE=%s", bronze_fqn)
    spark.stop()


if __name__ == "__main__":
    main()
