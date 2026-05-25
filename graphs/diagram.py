from diagrams import Cluster, Diagram
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import Spark, Superset
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL, Qdrant
from diagrams.onprem.mlops import Mlflow
from diagrams.onprem.monitoring import Grafana
from diagrams.onprem.queue import Kafka
from diagrams.onprem.workflow import Airflow


graph_attr = {
    "bgcolor": "#0c1b2a",
    "pad": "0.35",
    "nodesep": "0.7",
    "ranksep": "1.0",
    "splines": "ortho",
    "fontname": "Helvetica",
    "fontcolor": "white",
    "fontsize": "18",
}

node_attr = {
    "fontname": "Helvetica",
    "fontcolor": "white",
    "fontsize": "12",
}

edge_attr = {
    "color": "#e6f4ff",
    "penwidth": "2.0",
}

raw_cluster = {
    "style": "rounded,filled",
    "color": "#79b6ff",
    "fillcolor": "#17344b",
    "fontcolor": "white",
}

minio_cluster = {
    "style": "rounded,filled",
    "color": "#d96653",
    "fillcolor": "#6a3234",
    "fontcolor": "white",
}

bronze_cluster = {
    "style": "rounded,filled",
    "color": "#d7a65d",
    "fillcolor": "#6f5826",
    "fontcolor": "white",
}

silver_cluster = {
    "style": "rounded,filled",
    "color": "#b9c7d6",
    "fillcolor": "#6f7d88",
    "fontcolor": "white",
}

gold_cluster = {
    "style": "rounded,filled",
    "color": "#d8be43",
    "fillcolor": "#7d7024",
    "fontcolor": "white",
}

ai_cluster = {
    "style": "rounded,dashed,filled",
    "color": "#d8a9ff",
    "fillcolor": "#5f438e",
    "fontcolor": "white",
}

consume_cluster = {
    "style": "rounded,filled",
    "color": "#68c7e8",
    "fillcolor": "#183f58",
    "fontcolor": "white",
}


with Diagram(
    "DTL v3 Redraw",
    filename="graphs/dtlver3_redraw",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
    outformat="png",
):
    with Cluster("1. Data Ingestion (Raw Zone)", graph_attr=raw_cluster):
        iot = Kafka("IoT Devices\n(Kafka)")
        web_db = PostgreSQL("Web/App\nDatabases")

    with Cluster("MinIO Object Storage", graph_attr=minio_cluster):
        minio = Storage("MinIO S3\nBronze Bucket\ns3a://raw-landing/")

    airflow = Airflow("Airflow DAG\n(Orchestration)")

    with Cluster("2. Bronze Layer (Ingestion & Metadata)", graph_attr=bronze_cluster):
        bronze_spark = Spark("Spark + Iceberg")
        bronze_tables = Storage("Bronze Tables\n(Iceberg/Parquet)")
        iceberg_rest = Storage("Iceberg REST\nCatalog")
        bronze_spark >> bronze_tables
        bronze_tables - iceberg_rest

    with Cluster("3. Silver Layer (Transformation & Quality)", graph_attr=silver_cluster):
        silver_spark = Spark("Spark + Iceberg")
        silver_tables = Storage("Silver Tables\n(Iceberg/Parquet)")
        iceberg_catalog = Storage("Iceberg\nCatalog")
        silver_spark >> silver_tables
        silver_tables - iceberg_catalog

    with Cluster("4. Gold Layer (Aggregation & Insight)", graph_attr=gold_cluster):
        gold_spark = Spark("Spark + Iceberg")
        gold_tables = Storage("Gold Tables\n(Iceberg/Parquet)")
        gold_spark >> gold_tables

    with Cluster("AI Intelligence Layer (LangGraph)", graph_attr=ai_cluster):
        schema_agent = Server("Schema Inference Agent\n(LangGraph)")
        quality_agent = Server("Data Quality Agent\n(LangGraph)")
        nlsql_agent = Server("NL-to-SQL Agent\n(LangGraph/Vanna.ai)")
        semantic_agent = Server("Semantic Inquiry Agent\n(LangGraph)")
        ai_airflow = Airflow("Apache Airflow")

        schema_agent >> ai_airflow
        quality_agent >> ai_airflow
        nlsql_agent >> ai_airflow
        semantic_agent >> nlsql_agent

    with Cluster("5. Consumption & Data Science", graph_attr=consume_cluster):
        relational_db = PostgreSQL("Relational DB\n(PostgreSQL)")
        superset = Superset("Superset")
        grafana = Grafana("Grafana")
        jupyter = Server("Jupyter Notebooks\n(PySpark)")
        mlflow = Mlflow("MLflow")
        vector_db = Qdrant("Vector DB\n(Milvus)")

    [iot, web_db] >> minio
    [iot, web_db] >> airflow

    minio >> bronze_spark
    airflow >> bronze_spark
    bronze_tables >> schema_agent
    schema_agent >> iceberg_rest

    bronze_tables >> silver_spark
    airflow >> silver_spark
    iceberg_catalog >> quality_agent
    quality_agent >> silver_spark

    silver_tables >> gold_spark
    airflow >> gold_spark
    silver_tables >> nlsql_agent
    gold_tables >> nlsql_agent
    gold_tables >> semantic_agent

    gold_tables >> relational_db
    gold_tables >> superset
    gold_tables >> grafana
    silver_tables >> jupyter
    silver_tables >> mlflow
    gold_tables >> vector_db
