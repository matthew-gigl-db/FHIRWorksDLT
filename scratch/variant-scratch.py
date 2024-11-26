# Databricks notebook source
# MAGIC %md
# MAGIC Uncomment and run the following `%pip install` commands or set the Python Base Environment for the notebook using "resources/environment.yml".  

# COMMAND ----------

# %pip install databricks-sdk --upgrade

# COMMAND ----------

# %pip install git+https://github.com/databrickslabs/dbignite.git

# COMMAND ----------

from dbignite.fhir_mapping_model import FhirSchemaModel

# COMMAND ----------

# MAGIC %sql
# MAGIC USE redox.main;

# COMMAND ----------

df = spark.table("bundle_meta_parsed")
display(df)

# COMMAND ----------

grouping_cols = [col for col in df.columns if col not in ["pos", "key", "value"]]
grouping_cols

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df.select(*grouping_cols))

# COMMAND ----------

tdf = df.groupBy(*grouping_cols).pivot("key").agg(first("value"))

# COMMAND ----------

display(tdf)

# COMMAND ----------

cdc_df = spark.readStream.format("delta").option("readChangeData", "true").table("bundle_meta_parsed")

# COMMAND ----------

tcdf = cdc_df.groupBy(*grouping_cols).pivot("key").agg(first("value"))

# COMMAND ----------

distinct_keys = df.select("key").distinct().collect()
distinct_keys = sorted([row.key for row in distinct_keys])
distinct_keys

# COMMAND ----------

distinct_keys = sorted(distinct_keys)
distinct_keys

# COMMAND ----------

from pyspark.sql.functions import *

tdf2 = (
  df
  .groupBy(*grouping_cols)
  .agg(*[element_at(collect_list(when(col("key") == k, col("value"))), 1).alias(k) for k in distinct_keys])
)

# COMMAND ----------

display(tdf2)

# COMMAND ----------

stream_tdf = (
  spark.readStream.table("bundle_meta_parsed")
  .groupBy(*grouping_cols)
  .agg(
    *[element_at(
      collect_list(when(col("key") == k, col("value"))), 1
    ).alias(k) for k in distinct_keys]
  )
)

# COMMAND ----------

display(stream_tdf)

# COMMAND ----------

tdf_minus_tdf2 = tdf.select(*tdf2.columns).subtract(tdf2)
display(tdf_minus_tdf2)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("resources_parsed").filter(col("resourceType") == "Location")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC with locations as (
# MAGIC   SELECT * from redox.main.resources_parsed where resourceType = 'Location'
# MAGIC )
# MAGIC select 
# MAGIC   BundleUUID
# MAGIC   ,fullUrl
# MAGIC   ,key
# MAGIC   ,count(key) as cnt
# MAGIC from locations
# MAGIC group by BundleUUID, fullUrl, key
# MAGIC order by cnt desc

# COMMAND ----------


