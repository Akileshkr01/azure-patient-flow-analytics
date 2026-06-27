from pyspark.sql.types import *
from pyspark.sql.functions import *


spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

bronze_path = "abfss://bronze@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"
silver_path = "abfss://silver@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"

bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)


schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])


parsed_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")


clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))


clean_df = clean_df.withColumn("admission_time",
                               when(
                                   col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
                                   current_timestamp())
                               .otherwise(col("admission_time")))


clean_df = clean_df.withColumn("age",
                               when(col("age")>100,floor(rand()*90+1).cast("int"))
                               .otherwise(col("age"))
                               )


expected_cols = ["patient_id", "gender", "age", "department", "admission_time", "discharge_time", "bed_id", "hospital_id"]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))


checkpoint_path = "abfss://silver@<<Storageaccount_name>>.dfs.core.windows.net/checkpoints/patient_flow"

(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", checkpoint_path)
    .start(silver_path)
)