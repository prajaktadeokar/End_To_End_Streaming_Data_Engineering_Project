# Databricks notebook source
import pandas as pd
files= [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
    url = f"https://streamingstorage1.blob.core.windows.net/raw/Ingestion/{file['file']}.json?sp=r&st=2026-04-30T12:38:00Z&se=2026-04-30T20:53:00Z&spr=https&sv=2025-11-05&sr=c&sig=6%2BRYtxDUMGaTFw%2BxXPFji3egdvkLZ9mIRezpJRdn%2BxI%3D"
    #print(url)

    df= pd.read_json(url)
    spark_df= spark.createDataFrame(df)
    spark_df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber_catalog.bronze.{file['file']}")
        

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

import pandas as pd
files= [
{"file":"bulk_rides"},
]

for file in files:
    url = f"https://streamingstorage1.blob.core.windows.net/raw/Ingestion/{file['file']}.json?sp=r&st=2026-04-30T12:38:00Z&se=2026-04-30T20:53:00Z&spr=https&sv=2025-11-05&sr=c&sig=6%2BRYtxDUMGaTFw%2BxXPFji3egdvkLZ9mIRezpJRdn%2BxI%3D"
    #print(url)
    if not spark.catalog.tableExists(f"uber_catalog.bronze.{file['file']}"):
        df= pd.read_json(url)
        spark_df= spark.createDataFrame(df)
        spark_df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber_catalog.bronze.{file['file']}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.bronze.map_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

