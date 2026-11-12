"""
Shared SparkSession builder: wires up Delta, Iceberg (Hadoop catalog),
and the Kafka connector consistently across every streaming query script.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"
ICEBERG_WAREHOUSE = "./warehouse/iceberg"


def get_spark_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[ICEBERG_RUNTIME, KAFKA_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
