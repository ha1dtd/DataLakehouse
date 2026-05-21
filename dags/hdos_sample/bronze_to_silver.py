import logging
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, trim, upper, when

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from foxai_config import BRONZE_NAMESPACE, PG_SOURCE_TABLE, SILVER_NAMESPACE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_sample_bronze_to_silver")


def clean_string(column_name: str):
    return when(trim(col(column_name)) == "", None).otherwise(trim(col(column_name)))


def main() -> None:
    spark = SparkSession.builder.appName("hdos_sample_bronze_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bronze_fqn = f"bronze_catalog.{BRONZE_NAMESPACE}.{PG_SOURCE_TABLE}_bronze"
    silver_fqn = f"silver_catalog.{SILVER_NAMESPACE}.{PG_SOURCE_TABLE}_silver"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS silver_catalog.{SILVER_NAMESPACE}")

    base_df = spark.read.table(bronze_fqn)
    df = (
        base_df.filter(col("nhanvienlogid").isNotNull())
        .select(
            col("nhanvienlogid"),
            col("nhanvienid"),
            col("dm_nhanvientypeid"),
            col("logintime"),
            to_date(col("logintime")).alias("login_date"),
            clean_string("ipaddress").alias("ipaddress"),
            clean_string("internetip").alias("internetip"),
            clean_string("macadddress").alias("macadddress"),
            lower(clean_string("computername")).alias("computername"),
            clean_string("username").alias("username"),
            upper(clean_string("domain")).alias("domain"),
            clean_string("softversion").alias("softversion"),
            clean_string("softfolder").alias("softfolder"),
            col("version").alias("source_version_ts"),
            col("_ingested_at"),
        )
        .dropDuplicates(["nhanvienlogid"])
    )

    logger.info("SILVER_SOURCE=%s", bronze_fqn)
    logger.info("SILVER_TARGET=%s", silver_fqn)
    logger.info("SILVER_ROW_COUNT=%s", df.count())

    df.writeTo(silver_fqn).createOrReplace()
    logger.info("SILVER_WRITE_COMPLETE=%s", silver_fqn)
    spark.stop()


if __name__ == "__main__":
    main()
