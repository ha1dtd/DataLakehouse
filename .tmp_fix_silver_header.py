from pathlib import Path

path = Path('/Users/daihai/Documents/Code/FoxAI/dag_combined_domains/silver_from_bronze_domains.py')
text = path.read_text(encoding='utf-8')
idx = text.find('spark = SparkSession.builder')
if idx == -1:
    raise RuntimeError('marker not found')
header = '''import functools
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, lit, to_timestamp, unix_timestamp

from foxai_config import DOMAIN_REGISTRY_FILE, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

'''
path.write_text(header + text[idx:], encoding='utf-8')
print('silver header fixed')
