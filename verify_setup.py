from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"

builder = (
    SparkSession.builder.appName("verify-delta-iceberg")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "./warehouse/iceberg")
)

spark = configure_spark_with_delta_pip(builder, extra_packages=[ICEBERG_RUNTIME]).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])

df.write.format("delta").mode("overwrite").save("./warehouse/delta/test_table")
delta_read = spark.read.format("delta").load("./warehouse/delta/test_table")
print(f"[delta] row count: {delta_read.count()}")

df.writeTo("local.test_table").createOrReplace()
iceberg_read = spark.table("local.test_table")
print(f"[iceberg] row count: {iceberg_read.count()}")

spark.stop()
print("verify_setup: both formats read/wrote successfully")
