from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Spark, Superset
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL, Qdrant
from diagrams.onprem.mlops import Mlflow
from diagrams.onprem.monitoring import Grafana
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "dtlver3_redraw"

GRAPH_ATTR = {
    "bgcolor": "white",
    "pad": "0.3",
    "nodesep": "0.75",
    "ranksep": "0.9",
    "splines": "ortho",
}

NODE_ATTR = {
    "fontname": "Helvetica",
}

EDGE_ATTR = {
    "penwidth": "1.6",
}

RAW_CLUSTER = {
    "style": "rounded,filled",
    "fillcolor": "#f9d6d5",
    "color": "#d9534f",
}

BRONZE_CLUSTER = {
    "style": "rounded,filled",
    "fillcolor": "#f3e0c2",
    "color": "#b57f3f",
}

SILVER_CLUSTER = {
    "style": "rounded,filled",
    "fillcolor": "#eceff3",
    "color": "#9aa5b1",
}

GOLD_CLUSTER = {
    "style": "rounded,filled",
    "fillcolor": "#f7efb7",
    "color": "#c6a700",
}


with Diagram(
    "FoxAI Lakehouse Architecture",
    filename=str(OUTPUT_FILE),
    show=False,
    direction="LR",
    outformat="png",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    edge_attr=EDGE_ATTR,
):
    airflow = Airflow("Airflow\nOrchestration")

    with Cluster("Source Systems"):
        iot = Kafka("IoT Devices\nKafka")
        app_db = PostgreSQL("Web / App\nDatabases")

    with Cluster("Raw Zone", graph_attr=RAW_CLUSTER):
        raw_storage = Storage("MinIO Raw Landing")

    with Cluster("Bronze Layer", graph_attr=BRONZE_CLUSTER):
        bronze_job = Spark("Spark Ingestion")
        bronze_tables = Storage("Iceberg Bronze")

    with Cluster("Silver Layer", graph_attr=SILVER_CLUSTER):
        silver_job = Spark("Spark Transform")
        silver_tables = Storage("Iceberg Silver")

    with Cluster("Gold Layer", graph_attr=GOLD_CLUSTER):
        gold_job = Spark("Spark Aggregate")
        gold_tables = Storage("Iceberg Gold")

    with Cluster("AI Intelligence"):
        schema_agent = Server("Schema Inference\nAgent")
        quality_agent = Server("Data Quality\nAgent")
        semantic_agent = Server("Semantic Inquiry\nAgent")
        nlsql_agent = Server("NL-to-SQL\nAgent")

    with Cluster("Consumption & Data Science"):
        serving_db = PostgreSQL("Serving DB")
        superset = Superset("Superset")
        grafana = Grafana("Grafana")
        jupyter = Server("Jupyter\nNotebooks")
        mlflow = Mlflow("MLflow")
        vector_db = Qdrant("Vector DB")

    [iot, app_db] >> raw_storage
    airflow >> Edge(style="dashed") >> [bronze_job, silver_job, gold_job]

    raw_storage >> bronze_job >> bronze_tables >> silver_job >> silver_tables >> gold_job >> gold_tables

    bronze_tables >> schema_agent
    silver_tables >> quality_agent
    gold_tables >> semantic_agent
    [silver_tables, gold_tables] >> nlsql_agent
    semantic_agent >> nlsql_agent

    gold_tables >> serving_db >> [superset, grafana]
    gold_tables >> vector_db
    [silver_tables, gold_tables] >> jupyter
    [silver_tables, gold_tables] >> mlflow

    schema_agent >> Edge(style="invis") >> quality_agent
    quality_agent >> Edge(style="invis") >> semantic_agent
    semantic_agent >> Edge(style="invis") >> nlsql_agent
    serving_db >> Edge(style="invis") >> superset
    superset >> Edge(style="invis") >> grafana
    grafana >> Edge(style="invis") >> jupyter
    jupyter >> Edge(style="invis") >> mlflow
    mlflow >> Edge(style="invis") >> vector_db
