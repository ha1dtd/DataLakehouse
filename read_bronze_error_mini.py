from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadBronzeErrorMini") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

sc = spark.sparkContext
jvm = sc._jvm
hadoop_conf = sc._jsc.hadoopConfiguration()


def first_paths(pattern: str, limit: int = 10):
    path_obj = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path_obj.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path_obj)

    if not statuses:
        return []

    all_paths = sorted([status.getPath().toString() for status in statuses])
    return all_paths[:limit]


patterns = {
    "yellow_tripdata": "s3a://bronze/raw/yellow_tripdata_*.parquet",
    "green_tripdata": "s3a://bronze/raw/green_tripdata_*.parquet",
    "fhv_tripdata": "s3a://bronze/raw/fhv_tripdata_*.parquet",
    "fhvhv_tripdata": "s3a://bronze/raw/fhvhv_tripdata_*.parquet",
}

for name, pattern in patterns.items():
    print(f"### Bronze schema sample (mini): {name}")
    sample_file_paths = first_paths(pattern, 10)

    if not sample_file_paths:
        print(f"No files matched: {pattern}")
        print()
        continue

    print(f"Using {len(sample_file_paths)} sampled files")
    for idx, sample_path in enumerate(sample_file_paths, start=1):
        print(f"#### File {idx}: {sample_path}")
        df = spark.read.parquet(sample_path)
        df.printSchema()
        df.show(5, truncate=False)
        print()
