import logging
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from foxai_config import GOLD_NAMESPACE, PG_SOURCE_TABLE, SILVER_NAMESPACE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_sample_silver_to_gold")


def main() -> None:
    spark = SparkSession.builder.appName("hdos_sample_silver_to_gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_fqn = f"silver_catalog.{SILVER_NAMESPACE}.{PG_SOURCE_TABLE}_silver"
    gold_fqn = f"gold_catalog.{GOLD_NAMESPACE}.{PG_SOURCE_TABLE}_daily_domain_summary"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gold_catalog.{GOLD_NAMESPACE}")

    df = (
        spark.read.table(silver_fqn)
        .groupBy("login_date", "domain", "softversion")
        .agg(
            count("*").alias("login_count"),
            countDistinct("nhanvienid").alias("distinct_employee_count"),
            countDistinct("computername").alias("distinct_computer_count"),
            countDistinct("ipaddress").alias("distinct_ip_count"),
        )
        .orderBy(col("login_date").desc(), col("login_count").desc())
    )

    logger.info("GOLD_SOURCE=%s", silver_fqn)
    logger.info("GOLD_TARGET=%s", gold_fqn)
    logger.info("GOLD_ROW_COUNT=%s", df.count())

    df.writeTo(gold_fqn).createOrReplace()
    logger.info("GOLD_WRITE_COMPLETE=%s", gold_fqn)
    spark.stop()


if __name__ == "__main__":
    main()
