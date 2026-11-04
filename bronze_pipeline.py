

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

# Source JSON file
SOURCE_PATH = "/Volumes/main/default1/employee_files/employees.json"

# Bronze layer
@dp.materialized_view(
    name="bronze_employees",
    comment="Raw employee data ingested from Unity Catalog Volume"
)
def bronze_employees():

    employees_df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(SOURCE_PATH)
    )

    return (
        employees_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
