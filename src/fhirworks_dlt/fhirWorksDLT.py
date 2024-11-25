import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable
import os
import pandas as pd
from databricks.sdk import WorkspaceClient
# from databricks.sdk.runtime import *
# from dbignite.fhir_resource import FhirResource
# from dbignite.fhir_resource import BundleFhirResource
# from dbignite.fhir_mapping_model import FhirSchemaModel
import uuid

##########################################
### raw data ingestion with autoloader ###
##########################################
# read streaming data as whole text using autoloader    
def read_stream_raw(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, wholeText: bool = False, skipRows: int = 0, options: dict = None) -> DataFrame:
    stream_schema = "value STRING"
    read_stream = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", wholeText)
        .option("cloudFiles.maxBytesPerTrigger", maxBytes)
        .option("cloudFiles.maxFilesPerTrigger", maxFiles)
        .option("skipRows", skipRows)
    )

    if options is not None:
        read_stream = read_stream.options(**options)

    read_stream = (
        read_stream
        .schema(stream_schema)
        .load(path)
    )

    return read_stream

class fhirWorksDLTPipeline:

    def __init__(self, spark: SparkSession = SparkSession.getActiveSession()):
        self.spark = spark

class bronzePipeline(fhirWorksDLTPipeline):

    def __init__(self, spark: SparkSession, volume: str):
        super().__init__(spark)
        self.volume = volume

    def __repr__(self):
        return f"""fhirWorksDLT(volume='{self.volume}', quality='bronze')"""

    def raw_to_bronze(self, table_name: str, table_comment: str, table_properties: dict, source_folder_path_from_volume: str = "", maxFiles: int = 1000, maxBytes: str = "10g", options: dict = None):
        """
        Ingests all files in a volume's path to a key value pair bronze table.
        """
        variant_support = {"delta.feature.variantType-preview" : "supported"}
        table_properties.update(variant_support)

        @dlt.table(
            name = table_name
            ,comment = table_comment
            ,temporary = False
            ,table_properties = table_properties

        )
        def bronze_ingestion(spark = self.spark, source_folder_path_from_volume = source_folder_path_from_volume, maxFiles = maxFiles, maxBytes = maxBytes, options = options, volume = self.volume):

            if source_folder_path_from_volume == "":
                file_path = f"{volume}/"
            else:
                file_path = f"{volume}/{source_folder_path_from_volume}/"

            raw_df = read_stream_raw(spark = spark, path = file_path, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = True, options = options)

            bronze_df = (raw_df
                .withColumn("inputFilename", col("_metadata.file_name"))
                .withColumn("fullFilePath", col("_metadata.file_path"))
                .withColumn("fileMetadata", col("_metadata"))
                .withColumn("resource", parse_json(col("value")))
                .select(
                    "fullFilePath"
                    ,lit(file_path).alias("datasource")
                    ,"inputFileName"
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                    ,"resource"
                    ,"fileMetadata"
                ).withColumn("bundleUUID", expr("uuid()")) 
            )
            return bronze_df

class silverPipeline(fhirWorksDLTPipeline):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def __repr__(self):
        return f"""fhirWorksDLT(quality='silver')""" 
    
    def meta_stage_silver(
        self
        ,parsed_variant_meta_table: str
        ,meta_key_table: str
        ,live: bool = True
        ,temporary: bool = True
        ):
        @dlt.table(
            name = f"bundle_meta_stage".lower()
            ,comment = f"Staging Table for the latest streamed bundle metadata from the FHIR resource data recieved in bronze and variant exploded. This stage table transposes the exploded variant data wide for easier access.  Normally temporary."
            ,temporary = temporary
            ,table_properties = {
                "pipelines.autoOptimize.managed" : "true"
                ,"pipelines.reset.allowed" : "true"
                ,"delta.feature.variantType-preview" : "supported"
            }
        )
        def stage_meta():
            if live:
                src_tbl_name = f"LIVE.{parsed_variant_meta_table}"
            else:
                src_tbl_name = f"{parsed_variant_meta_table}"

            sdf = self.spark.readStream.table(src_tbl_name)
            grouping_cols = [col for col in sdf.columns if col not in ["pos", "key", "value"]]

            distinct_keys = self.spark.table(meta_key_table).select("key").distinct().collect()
            distinct_keys = sorted([row.key for row in distinct_keys])

            # distinct_keys = ['DataModel','Destinations','EventDateTime','EventType','FacilityCode','Logs','Message','Source','Test','Transmission']

            return (
                sdf
                .groupBy(*grouping_cols)
                .agg(
                    *[element_at(
                        collect_list(when(col("key") == k, col("value"))), 1
                    ).alias(k) for k in distinct_keys]
                )
            )
    
