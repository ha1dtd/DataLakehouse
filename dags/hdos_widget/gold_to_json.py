import json
import logging
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max, sum as spark_sum

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from hdos_widget_config import GOLD_NAMESPACE, JSON_EXPORT_BASE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_widget_gold_to_json")


def gold_table(table_name: str) -> str:
    return f"gold_catalog.{GOLD_NAMESPACE}.{table_name}"


def json_target_path(screen_id: str) -> str:
    base = JSON_EXPORT_BASE.rstrip("/")
    return f"{base}/screen/{screen_id}.json"


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    target_fqn = gold_table(table_name)
    logger.info("READ_GOLD_TABLE=%s", target_fqn)
    return spark.read.table(target_fqn)


def latest_date_value(df: DataFrame, column_name: str):
    row = df.select(spark_max(col(column_name)).alias("latest_value")).collect()[0]
    return row["latest_value"]


def decimal_or_zero(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def round_half_up(value: Decimal, places: str) -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def format_short_currency(value: Decimal) -> str:
    abs_value = abs(value)
    if abs_value >= Decimal("1000000000"):
        return f"{round_half_up(value / Decimal('1000000000'), '0.01')} tỷ"
    if abs_value >= Decimal("1000000"):
        return f"{round_half_up(value / Decimal('1000000'), '0.01')} triệu"
    return f"{int(value):,} VND".replace(",", ".")


def ratio_color(ratio_percent: Decimal) -> str:
    if ratio_percent >= Decimal("85"):
        return "#ff4d4f"
    if ratio_percent >= Decimal("70"):
        return "#fa8c16"
    return "#52c41a"


def to_int(value) -> int:
    return int(decimal_or_zero(value))


def to_float_2(value) -> float:
    return float(round_half_up(decimal_or_zero(value), "0.01"))


def to_percent_value(value) -> Decimal:
    return round_half_up(decimal_or_zero(value) * Decimal("100"), "0.1")


def build_kpi_components(encounter_df: DataFrame, finance_df: DataFrame, inpatient_df: DataFrame, bed_df: DataFrame) -> list[dict]:
    latest_encounter_date = latest_date_value(encounter_df, "encounter_date")
    latest_invoice_date = latest_date_value(finance_df, "invoice_date")
    latest_inpatient_date = latest_date_value(inpatient_df, "encounter_date")
    latest_bed_snapshot = latest_date_value(bed_df, "snapshot_date")

    encounter_totals = (
        encounter_df.filter(col("encounter_date") == latest_encounter_date)
        .agg(
            spark_sum("encounter_count").alias("encounter_count"),
            spark_sum("distinct_patient_count").alias("distinct_patient_count"),
            spark_sum("insured_encounter_count").alias("insured_encounter_count"),
        )
        .collect()[0]
    )
    finance_totals = (
        finance_df.filter(col("invoice_date") == latest_invoice_date)
        .agg(
            spark_sum("total_invoice_amount").alias("total_invoice_amount"),
            spark_sum("total_discount_amount").alias("total_discount_amount"),
        )
        .collect()[0]
    )
    inpatient_totals = (
        inpatient_df.filter(col("encounter_date") == latest_inpatient_date)
        .agg(
            spark_sum("inpatient_encounter_count").alias("inpatient_encounter_count"),
            spark_sum("discharged_encounter_count").alias("discharged_encounter_count"),
        )
        .collect()[0]
    )
    bed_totals = (
        bed_df.filter(col("snapshot_date") == latest_bed_snapshot)
        .agg(
            spark_sum("available_bed_count").alias("available_bed_count"),
            spark_sum("occupied_bed_count").alias("occupied_bed_count"),
        )
        .collect()[0]
    )

    total_revenue = decimal_or_zero(finance_totals["total_invoice_amount"])
    total_discount = decimal_or_zero(finance_totals["total_discount_amount"])
    available_beds = decimal_or_zero(bed_totals["available_bed_count"])
    occupied_beds = decimal_or_zero(bed_totals["occupied_bed_count"])
    bor_percent = Decimal("0.0") if available_beds == 0 else round_half_up((occupied_beds / available_beds) * Decimal("100"), "0.1")

    return [
        {
            "type": "KpiCard",
            "props": {
                "title": "LƯỢT KHÁM HÔM NAY",
                "value": to_int(encounter_totals["encounter_count"]),
                "accent": "#1677ff",
                "hint": f"{to_int(encounter_totals['distinct_patient_count'])} bệnh nhân",
                "hintColor": "#1677ff",
            },
        },
        {
            "type": "KpiCard",
            "props": {
                "title": "DOANH THU",
                "value": format_short_currency(total_revenue),
                "accent": "#52c41a",
                "hint": f"Giảm trừ {format_short_currency(total_discount)}",
                "hintColor": "#8c8c8c",
            },
        },
        {
            "type": "KpiCard",
            "props": {
                "title": "BN NỘI TRÚ",
                "value": to_int(inpatient_totals["inpatient_encounter_count"]),
                "accent": "#722ed1",
                "hint": f"Ra viện {to_int(inpatient_totals['discharged_encounter_count'])}",
                "hintColor": "#8c8c8c",
            },
        },
        {
            "type": "KpiCard",
            "props": {
                "title": "BOR TOÀN VIỆN",
                "value": f"{bor_percent}%",
                "accent": "#fa8c16",
                "hint": f"{int(occupied_beds)}/{int(available_beds)} giường sử dụng",
                "hintColor": ratio_color(bor_percent),
            },
        },
    ]


def build_bed_progress_component(bed_df: DataFrame) -> dict:
    latest_snapshot = latest_date_value(bed_df, "snapshot_date")
    rows = (
        bed_df.filter(col("snapshot_date") == latest_snapshot)
        .orderBy(col("occupancy_ratio").desc_nulls_last(), col("occupied_bed_count").desc())
        .limit(10)
        .collect()
    )
    items = []
    for row in rows:
        ratio_percent = to_percent_value(row["occupancy_ratio"])
        items.append(
            {
                "label": row["departmentname"] or row["departmentcode"] or f"Khoa {row['departmentid']}",
                "value": to_float_2(ratio_percent),
                "secondaryValue": 100,
                "color": ratio_color(ratio_percent),
            }
        )
    return {
        "type": "ProgressList",
        "span": 16,
        "props": {
            "title": "Công suất giường theo khoa",
            "maxValue": 100,
            "items": items,
            "footerActions": [{"label": f"{len(items)} khoa nổi bật", "variant": "link"}],
        },
    }


def finance_bucket_label(raw_value) -> str:
    if raw_value is None:
        return "Không xác định"
    text = str(raw_value).strip()
    if text.startswith("invoice_type_"):
        suffix = text.removeprefix("invoice_type_")
        return f"Nhóm hóa đơn {suffix}"
    return text


def build_finance_pie_component(finance_df: DataFrame) -> dict:
    latest_invoice_date = latest_date_value(finance_df, "invoice_date")
    rows = (
        finance_df.filter(col("invoice_date") == latest_invoice_date)
        .groupBy("finance_bucket")
        .agg(spark_sum("total_invoice_amount").alias("bucket_amount"))
        .orderBy(col("bucket_amount").desc())
        .collect()
    )
    data = [{"label": finance_bucket_label(row["finance_bucket"]), "value": to_float_2(row["bucket_amount"])} for row in rows]
    return {
        "type": "ChartPie",
        "span": 8,
        "props": {
            "title": "Phân loại doanh thu",
            "height": 220,
            "variant": "donut",
            "legend": True,
            "data": data,
            "colors": ["#1677ff", "#fa8c16", "#52c41a", "#8b949e", "#722ed1"],
        },
    }


def build_flow_pipeline_component(encounter_df: DataFrame, inpatient_df: DataFrame) -> dict:
    latest_encounter_date = latest_date_value(encounter_df, "encounter_date")
    latest_inpatient_date = latest_date_value(inpatient_df, "encounter_date")

    encounter_totals = (
        encounter_df.filter(col("encounter_date") == latest_encounter_date)
        .agg(
            spark_sum("encounter_count").alias("encounter_count"),
            spark_sum("inpatient_encounter_count").alias("inpatient_encounter_count"),
            spark_sum("discharged_encounter_count").alias("discharged_encounter_count"),
            spark_sum("insured_encounter_count").alias("insured_encounter_count"),
        )
        .collect()[0]
    )
    inpatient_totals = (
        inpatient_df.filter(col("encounter_date") == latest_inpatient_date)
        .agg(spark_sum("distinct_patient_count").alias("distinct_patient_count"))
        .collect()[0]
    )

    stages = [
        {"label": "Tổng lượt", "value": to_int(encounter_totals["encounter_count"]), "color": "#1677ff"},
        {"label": "Nội trú", "value": to_int(encounter_totals["inpatient_encounter_count"]), "color": "#722ed1"},
        {"label": "Ra viện", "value": to_int(encounter_totals["discharged_encounter_count"]), "color": "#52c41a"},
        {"label": "Có BHYT", "value": to_int(encounter_totals["insured_encounter_count"]), "color": "#fa8c16"},
    ]
    return {
        "type": "FlowPipeline",
        "span": 16,
        "props": {
            "title": "Dòng bệnh nhân trong ngày",
            "footer": f"{to_int(inpatient_totals['distinct_patient_count'])} bệnh nhân phân biệt trong lớp dữ liệu mới nhất",
            "stages": stages,
        },
    }


def build_clinical_pathway_component(pathway_df: DataFrame) -> dict:
    rows = pathway_df.orderBy(col("pathway_sheet_count").desc(), col("tenphacdo").asc_nulls_last()).limit(8).collect()
    max_value = max((to_float_2(row["pathway_sheet_count"]) for row in rows), default=0.0)
    items = []
    for row in rows:
        items.append(
            {
                "label": row["tenphacdo"] or row["maphacdo"] or f"Phác đồ {row['phacdodieutriid']}",
                "value": to_float_2(row["pathway_sheet_count"]),
                "secondaryValue": to_float_2(row["configured_treatment_days"]),
                "color": "#722ed1",
            }
        )
    return {
        "type": "ProgressList",
        "span": 8,
        "props": {
            "title": "Phác đồ điều trị nổi bật",
            "maxValue": max_value if max_value > 0 else 1,
            "items": items,
            "footerActions": [{"label": f"{len(items)} phác đồ có hoạt động", "variant": "link"}],
        },
    }


def build_dashboard_payload(spark: SparkSession) -> dict:
    encounter_df = read_gold_table(spark, "dashboard_daily_encounter_activity")
    finance_df = read_gold_table(spark, "dashboard_daily_finance_classification")
    inpatient_df = read_gold_table(spark, "dashboard_daily_inpatient_summary")
    bed_df = read_gold_table(spark, "dashboard_department_bed_occupancy")
    pathway_df = read_gold_table(spark, "dashboard_clinical_pathway_summary")

    return {
        "title": "Executive Dashboard",
        "badge": "HDOS Widget",
        "live": False,
        "subtitle": "Tổng quan điều hành toàn viện từ lớp Gold Lakehouse",
        "actions": [
            {"label": "↺ Làm mới", "variant": "default"},
            {"label": "Báo cáo giao ban", "variant": "default"},
            {"label": "Hỏi AI", "variant": "primary", "color": "#1677ff"},
        ],
        "rows": [
            {"components": build_kpi_components(encounter_df, finance_df, inpatient_df, bed_df)},
            {"components": [build_bed_progress_component(bed_df), build_finance_pie_component(finance_df)]},
            {"components": [build_flow_pipeline_component(encounter_df, inpatient_df), build_clinical_pathway_component(pathway_df)]},
        ],
    }


def build_widget_definition(
    widget_key: str,
    title: str,
    subtitle: str,
    chart_type: str,
    grid_x: int,
    grid_y: int,
    grid_w: int,
    grid_h: int,
    operation_pattern: str,
    provider_id: str,
    params_template: dict,
    visual_config: dict,
) -> dict:
    return {
        "widgetKey": widget_key,
        "title": title,
        "subtitle": subtitle,
        "chartType": chart_type,
        "gridX": grid_x,
        "gridY": grid_y,
        "gridW": grid_w,
        "gridH": grid_h,
        "operationPattern": operation_pattern,
        "providerId": provider_id,
        "paramsTemplate": json.dumps(params_template, ensure_ascii=False, separators=(",", ":")),
        "visualConfig": json.dumps(visual_config, ensure_ascii=False, separators=(",", ":")),
        "filterBindings": [],
        "interactions": "{}",
        "filterKey": "",
    }


def build_dashboard_fe_payload(spark: SparkSession) -> dict:
    hydrated = build_dashboard_payload(spark)
    overview_components = hydrated["rows"][0]["components"]
    bed_component = hydrated["rows"][1]["components"][0]
    donut_component = hydrated["rows"][1]["components"][1]
    stats_component = hydrated["rows"][2]["components"][0]
    pathway_component = hydrated["rows"][2]["components"][1]

    widgets = [
        build_widget_definition(
            "kpi_visits",
            overview_components[0]["props"]["title"],
            overview_components[0]["props"]["hint"],
            "kpi",
            0,
            0,
            2,
            2,
            "patient.list",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "kpi_visits"},
            {"color": "blue"},
        ),
        build_widget_definition(
            "kpi_revenue",
            overview_components[1]["props"]["title"],
            overview_components[1]["props"]["hint"],
            "kpi",
            2,
            0,
            3,
            2,
            "finance.classification",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "kpi_revenue"},
            {"color": "green"},
        ),
        build_widget_definition(
            "kpi_inpatient",
            overview_components[2]["props"]["title"],
            overview_components[2]["props"]["hint"],
            "kpi",
            5,
            0,
            2,
            2,
            "patient.inpatient",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "kpi_inpatient"},
            {"color": "purple"},
        ),
        build_widget_definition(
            "kpi_bor",
            overview_components[3]["props"]["title"],
            overview_components[3]["props"]["hint"],
            "kpi",
            7,
            0,
            3,
            2,
            "bed.occupancy",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "kpi_bor"},
            {"color": "orange"},
        ),
        build_widget_definition(
            "prog_capacity",
            bed_component["props"]["title"],
            "",
            "progress_rows",
            0,
            2,
            7,
            7,
            "bed.occupancy",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "prog_capacity"},
            {},
        ),
        build_widget_definition(
            "stats_flow",
            stats_component["props"]["title"],
            stats_component["props"]["footer"],
            "stats_summary",
            0,
            9,
            7,
            3,
            "patient.list",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "stats_flow"},
            {},
        ),
        build_widget_definition(
            "donut_rev",
            donut_component["props"]["title"],
            "",
            "donut_chart",
            7,
            9,
            5,
            6,
            "finance.classification",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "donut_rev"},
            {},
        ),
        build_widget_definition(
            "prog_pathway",
            pathway_component["props"]["title"],
            "",
            "progress_rows",
            0,
            15,
            12,
            5,
            "clinical.pathway",
            "lakehouse-api",
            {"screen": "dashboard", "widgetKey": "prog_pathway"},
            {},
        ),
    ]

    return {
        "dashboard": {
            "slug": "dashboard",
            "label": "Executive Dashboard v1",
            "description": "Tổng quan điều hành toàn viện · Cập nhật realtime",
            "tabs": [
                {
                    "id": "db-tab-1",
                    "slug": "overview",
                    "label": "Tổng quan",
                    "sortOrder": 0,
                    "isDefault": True,
                    "widgets": widgets,
                },
                {
                    "id": "db-tab-2",
                    "slug": "finance",
                    "label": "Tài chính",
                    "sortOrder": 1,
                    "isDefault": False,
                    "widgets": [],
                },
            ],
        }
    }


def write_text_to_path(spark: SparkSession, target_path: str, payload: str) -> None:
    jvm = spark._jvm
    jsc = spark.sparkContext._jsc
    hadoop_conf = jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(target_path)
    file_system = path.getFileSystem(hadoop_conf)
    parent = path.getParent()
    if parent is not None:
        file_system.mkdirs(parent)
    writer = None
    output_stream = None
    try:
        output_stream = file_system.create(path, True)
        writer = jvm.java.io.OutputStreamWriter(output_stream, "UTF-8")
        writer.write(payload)
        writer.flush()
    finally:
        if writer is not None:
            writer.close()
        elif output_stream is not None:
            output_stream.close()


def write_json_snapshot(spark: SparkSession, screen_id: str, payload: dict) -> None:
    target_path = json_target_path(screen_id)
    json_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    logger.info("JSON_TARGET=%s", target_path)
    logger.info("JSON_PAYLOAD_BYTES=%s", len(json_payload.encode("utf-8")))
    write_text_to_path(spark, target_path, json_payload)
    logger.info("JSON_WRITE_COMPLETE=%s", target_path)


def main() -> None:
    spark = SparkSession.builder.appName("hdos_widget_gold_to_json").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    write_json_snapshot(spark, "dashboard", build_dashboard_payload(spark))
    write_json_snapshot(spark, "dashboard_fe", build_dashboard_fe_payload(spark))

    spark.stop()


if __name__ == "__main__":
    main()
