from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, upper


# Silver layer
@dp.materialized_view(
    name="silver_employees",
    comment="Cleaned and standardized employee data"
)
@dp.expect("non_negative_salary", "salary >= 0")
def silver_employees():

    # Read Bronze data
    bronze_df = spark.read.table("bronze_employees")

    # Clean and standardize
    silver_df = (
        bronze_df
        .withColumn("name", trim(col("name")))
        .withColumn("department", upper(trim(col("department"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("salary", col("salary").cast("double"))
        .dropDuplicates(["employee_id"])
        .filter(
            col("employee_id").isNotNull()
            & (col("employee_id") != "")
            & col("name").isNotNull()
            & (col("name") != "")
            & col("salary").isNotNull()
        )
    )

    return silver_df
