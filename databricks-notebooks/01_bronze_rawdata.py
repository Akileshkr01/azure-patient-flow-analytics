from pyspark.sql.functions import *


event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name="<<Eventhub_Name>>"  
event_hub_conn_str = "<<Connection_string>>"


kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )


json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

bronze_path = "abfss://bronze@<<Storageaccount_name>>.dfs.core.windows.net/patient_flow"




checkpoint_path = "abfss://bronze@<<Storageaccount_name>>.dfs.core.windows.net/_checkpoints/patient_flow"

(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)