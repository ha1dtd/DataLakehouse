FILE MANAGEMENT, METADATA, CLASSIFICATION AND VERSIONING DURING INGESTION (L0)

1. Introduction
   1.1 Purpose
   The purpose of this document is to define the requirements for a real-time data ingestion pipeline that handles diverse data types (structured and unstructured) while maintaining metadata integrity, classification, and versioning using a Medallion Architecture (Raw Bronze, Parquet Bronze, and Silver).
   1.2 Intended Audience
   Data Engineers and Architects.
   System Integrators.
   DevOps Engineers managing MinIO and Kafka infrastructure.
   1.3 Intended Use
   This document serves as the primary technical reference for implementing the ingestion logic, storage folder hierarchies, and schema evolution strategies.
   1.4 Product Scope
   The system covers data ingestion from source (Kafka, Debezium, Manual) through the transformation into optimized Iceberg tables in the Silver layer. It includes real-time capture, batch compaction, and metadata recording.
   1.5 Definitions and Acronyms
   CDC: Change Data Capture.
   L0: Level 0 (Initial Ingestion Layer).
   MIME: Multipurpose Internet Mail Extensions (used for file classification).
   S3: Simple Storage Service (Object Storage).
   DAG: Directed Acyclic Graph (Workload Orchestration).

2. Overall Description
   2.1 User Needs
   Real-time recording of all incoming data types, including database changes.
   Programmatic organization of data based on stream type and Kafka topics.
   Automated versioning and schema evolution tracking.
   A centralized repository for structured and unstructured data.
   2.2 Assumptions and Dependencies
   Debezium is configured for CDC tasks.
   Kafka is the primary broker for data streams.
   Apache Iceberg is utilized for automated versioning and schema management.
   MinIO serves as the S3-compatible storage backend.

3. System Features and Requirements
   3.1 Functional Requirements
   3.1.1 Ingestion and Classification
   FR-1: The system shall ingest long CSVs, short JSONs, image byte streams, and video byte streams.
   FR-2: The system shall classify incoming data by MIME type and assign it to "unstructured" or "structured" directories.
   3.1.2 EARS Format
   Event-Driven Classification: When a Kafka topic message is received, the system shall use the topic name to assign the file to its proper folder location.
   Batch Processing: When a set time interval or manual DAG trigger occurs, the system shall pack CSV and JSON files into Parquet format.
   Schema Evolution: When the schema of daily intake differs from existing data, the system shall trigger Apache Iceberg to create a schema evolution JSON.
   3.1.3 Specification by Example (Gherkin)
   Scenario: Packing raw files into Parquet
   Given multiple .csv and .json files exist in raw_bronze/date/loans/
   When the daily batch trigger occurs
   Then the system should execute unionByName() on the dataframes
   And write the output as a partitioned Parquet file in parquet_bronze/date/loans/
   3.2 Non-Functional Requirements
   3.2.1 Performance
   Data Organization: Incoming data must be analyzed programmatically to determine location assignments without human intervention.
   Atomicity: Folder reorganizations must ensure atomic moves to avoid partial writes or updates.
   3.2.2 Security
   (Requirement not provided in source text—TBD)
   3.2.3 Usability, Reliability, Compliance
   Reliability: Technical info (size, source, time) must be recorded in both standard and Iceberg metadata formats.
   3.3 External Interface Requirements
   S3 Interface: All data must be accessible via s3a:// protocols for storage in MinIO.
   Metadata Interface: System must support metadata recording for schema, source, ingestion time, and domain.
   3.4 System Features
   Manual Override: Category classification can be managed via a manual configuration file.
   Data Normalization: Capabilities include column renaming, adding columns via preexisting schemas, or typecasting based on data samples.

4. Other Requirements
   4.1 Database Requirements
   CDC Capture: Integration with Debezium to record all database changes into the accounts path.
   4.2 Legal and Regulatory Requirements
   (Requirement not provided in source text—TBD)
   4.3 Internationalization and Localization
   (Requirement not provided in source text—TBD)
   4.4 Risk Management (FMEA Matrix)
   Risk: Partial writes during data moves. Mitigation: Implementation of atomic moves and Iceberg's ACID transaction capabilities.

5. Appendices
   5.1 Glossary
   Bronze Layer: Raw, uncleaned data in its original format.
   Silver Layer: Validated, cleaned, and optimized data.
   Iceberg: Open table format for huge analytic datasets.
   5.2 Use Cases and Diagrams
   Data Flow Architecture:
   Ingestion: Kafka/Debezium -> raw_bronze (Native format).
   Compaction: raw_bronze -> parquet_bronze (Parquet + Metadata).
   Refinement: parquet_bronze -> silver (Iceberg Table + Schema Evolution).
   5.3 To Be Determined (TBD) List
   Specific interval for automated batch triggers (Daily vs. Hourly).
   Security protocols for S3 bucket access.
   Legal/Compliance retention policies for unstructured blobs (Images/Video).

Goals and Requirements:
Ingest data in real time and organize it. Record and store all incoming data types including database changes.
Debezium to record CDC.
V2: Kafka to record data streams of unstructured and structured data.
Incoming data gets analyzed programmatically using the stream data type and kafka topic to be assigned locations.
Category classification can be done with a manual config file.
Record metadata of all data: schema, source, ingestion time, other metadata for all coalesced data.
Schema, source, size, ingestion time, domain and technical info can be obtained in the normal and Iceberg metadata in bronze.  
Record timestamps for schema evolution and versioning.
Versioning with timestamps can be done automatically by Iceberg.

FLOW OF DATA:
Incoming data can be in the form of long csvs with multiple rows, short jsons of CDC or realtime data points and rows, image byte streams, video byte streams, etc.
Incoming data through kafka, debezium or manual ingestion gets inspected and normalized.
Normalization can be done with renaming and adding columns using preexisting schema, or typecasting using a preexisting data sample  
schema = df.schema, df = df.withColumnRenamed(input_column, normalized_column)
SELECT \* FROM parquet.’s3a://parquet_bronze/date/’ LIMIT 10;).
Ingested data gets classified by mime type, then placed into unstructured or structured data.
V2: The kafka topic can be used to assign the proper folder.

MinIO S3 Storage
├── raw_bronze ( raw real time ingestion)
│ ├── 7-4-2026
│ │ │
│ │ ├── loans (Kafka Stream)
│ │ │ ├── …
│ │ │ ├── lvb-loan-7-4-2026-07:25:31.json
│ │ │ └── lvb-loans-7-4-2026-evening.csv
│ │ │
│ │ ├── accounts (DB CDC)
│ │ │ └──…
│ │ │
│ │ ├── transactions (Kafka Stream)
│ │ │ ├── …
│ │ │ ├── lvb-transactions-7-4-2026-morning.csv
│ │ │ └── lvb-transaction-7-4-2026-08:17:05.json
│ │ │
│ │ ├── …
│ │ │
│ │ └── unstructured_data ( Kafka Stream blobs)
│ │ ├── images
│ │ │ ├── CCCD_Nguyen_Thi_X-3-4-2026.png
│ │ │ ├── XXXXXXXXX_Check_Nguyen_Chi-Z-7-4-2026.jpg
│ │ │ └──…
│ │ │
│ │ ├── audio
│ │ │ └──…
│ │ │
│ │ └── videos
│ │ └──…
│ │
│ └── …
│

By a set time interval (daily, hourly, …), or by manual trigger (DAG trigger), the csv and json files are packed into parquet files.
batch_df = csv_df.unionByName(json_df),
batch_df.write.mode("append").partitionBy("ingestion_time").parquet("s3a://parquet_bronze/date/")
There is no need for schema evolution yet for daily data intake. The schemas are combined in the previous step.

MinIO S3 Storage
├── parquet_bronze
│ ├── 7-4-2026
│ │ │
│ │ ├── loans
│ │ │ └── lvb-loans-7-4-2026.parquet
│ │ │
│ │ ├── accounts
│ │ │ └──…
│ │ │
│ │ ├── transactions
│ │ │ └── lvb-transactions-7-4-2026.parquet
│ │ │
│ │ ├── …
│ │ │
│ │ └── unstructured_data
│ │
│ └── …
│

The folders are reorganized, ensure atomic moves to avoid partial writes and updates.
The normalized schema for each day or each point in time may differ, so Iceberg will be configured to create a schema evolution json with recording schemas through date.

Validate, clean, transform, combine and optimize data.

MinIO S3 Storage
├── silver
│ ├── loans
│ │ ├── lvb-loans-metadata.json
│ │ ├── lvb-loans-6-4-2026.parquet
│ │ └── lvb-loans-7-4-2026.parquet
│ │
│ ├── accounts
│ │ └──…
│ │
│ ├── transactions
│ │ ├── lvb-transactionss-metadata.json
│ │ ├── lvb-transactions-6-4-2026.parquet
│ │ └── lvb-transactions-7-4-2026.parquet
│ │
│ ├── …
│ │
│ └── unstructured_data
│

DRAFT:

Columns:
dataset_id STRING
file_name STRING
source STRING
format STRING
ingestion_time TIMESTAMP
domain STRING (initially null, updated after model)
storage_path STRING
schema_json STRING
size INTEGER
Script:
Read raw files from L1 (data source) folders.
Extract schema (JSON format) and relevant info.
Insert into `metadata` table.
This script should be idempotent (can run multiple times without duplication).

PART 2: AUTOMATED DOMAIN/TERM CATEGORIZATION MODEL (L0)
Goal:
Auto-classify datasets by department/domain using a machine learning or rules-based model.

Requirements:
Model can be general (ML/NLP/rules) and outputs a domain label for each dataset.
Script should:
Read untagged entries from `metadata`.
Predict domain/tag.
Update `metadata.domain` for each entry.
Must handle new datasets incrementally.
Ensure that each dataset gets **exactly one domain** to avoid overlap.
Example pseudo-code:
def categorize_metadata():
for row in metadata_table.filter(domain IS NULL):
predicted_domain = model.predict(row.schema_json, row.file_name)
update_metadata(row.dataset_id, domain=predicted_domain)

PART 3: FOLDER STRUCTURE ABSTRACTION (L1-L2)
Goal:
Organize bronze data by domain to isolate departmental datasets.

Requirements:
Base S3 bucket: s3a://bronze/
Each domain gets its own subfolder:
s3a://bronze/<domain*name>/raw/
Ingestion script should:
Read `metadata` table.
Copy/move each file from L1 source folder to corresponding `domain/raw/`.
Ensure **atomic move** to avoid partial updates.
Maintain consistent naming conventions:
<dataset_id>*<file_name>.<format>

Example pseudo-code:
for row in metadata*table:
target_path = f"s3a://bronze/{row.domain}/raw/{row.dataset_id}*{row.file_name}"
move_file(row.storage_path, target_path)
update_metadata(row.dataset_id, storage_path=target_path)

PART 4: DOMAIN-AWARE DATA LANDING (L0)
Goal:
Ensure data lands in the correct domain-specific bronze folder after categorization.

Requirements:
Modified ingest scripts should:
Read from domain-specific raw folders.
Write to domain-specific bronze tables:
bronze*catalog.<domain>.raw*<table_name>
Validate schema against metadata before insert.
Scripts must prevent cross-domain writes.
Maintain a consistent schema registry per domain.

Example pseudo-code:
for domain in get_all_domains():
files = list_files(f"s3a://bronze/{domain}/raw/")
for file in files:
insert_into_iceberg(f"bronze_catalog.{domain}.raw_ingested", file)

PART 5: DOMAIN-SEGREGATED BRONZE → SILVER → GOLD PROCESSING
Goal:
Each domain independently processes its datasets through the layers without overlaps.

Requirements:
For each domain:
Bronze:
Store raw ingested files.
Silver:
Clean, transform, and join domain data.
Table: silver_catalog.<domain>.<table_name>
Gold:
Aggregated, curated tables for analytics/BI.
Table: gold_catalog.<domain>.<table_name>
Airflow DAGs:
DAG per domain or parameterized by domain.
Steps: 1. Run L1-L2 ingestion (metadata update + file move) 2. Transform bronze → silver 3. Transform silver → gold 4. Optional: trigger downstream consumption or API exposure
Logging & monitoring:
Each DAG logs per domain run.
Schema validation at every step.
Failures must not affect other domains.

NOTES:
Scripts should be modular: metadata_ingest.py, categorize.py, move_to_domain.py, bronze_to_silver.py, silver_to_gold.py
Airflow:
Tasks orchestrate scripts per domain.
Parameterized DAGs preferred to reduce duplication.
S3/warehouse paths must match domain folder structure.
Iceberg catalog configurations remain per catalog, but tables are namespaced by domain.
All operations must be idempotent to allow retries.
