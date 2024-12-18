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
from databricks.sdk.runtime import *
from dbignite.fhir_resource import FhirResource
from dbignite.fhir_resource import BundleFhirResource
from dbignite.fhir_mapping_model import FhirSchemaModel
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

########################################## 
### ingestion pipleine class defintion ###
########################################## 
class ignitePipeline:

    def __init__(
        self
        ,spark: SparkSession # = spark
        ,volume: str
    ):
        self.spark = spark
        self.volume = volume

    def __repr__(self):
        return f"""fhirIngestionDLT(volume='{self.volume}')"""

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
        
    def meta_stage_silver(
        self
        ,parsed_variant_meta_table: str
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

            distinct_keys = self.spark.table(src_tbl_name).select("key").distinct().collect()
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

#########################################################################
# original classes methods below -- moving to scratch in future commits #
#########################################################################

# ###########################
# ### dbignite subclasses ###
# ###########################
# class StreamingFhir(FhirResource):
#     ### Note:  The FHIR resource must only contain only the "BUNDLE" resource type.  
#     def from_raw_bundle_resource(data: DataFrame) -> "FhirResource":
#         resources_df = data.withColumn("resourceType", get_json_object("resource", "$.resourceType"))
#         # resources_df = data.select(col("resource"), get_json_object("resource", "$.resourceType").alias("resourceType"))
#         return StreamingBundleFhirResource(resources_df.filter("upper(resourceType) == 'BUNDLE'"))

# class StreamingBundleFhirResource(BundleFhirResource):
#     # Note: Redox uses entry.fullUrl for primary keys
#     # BUNDLE_SCHEMA = (
#     #     StructType()
#     #      .add("resourceType", StringType())
#     #      .add("entry", ArrayType(
#     #          StructType()
#     #           .add("resource", StringType())
#     #           .add("fullUrl", StringType())
#     #      ))
#     #      .add("id", StringType())
#     #      .add("timestamp", StringType())
#     # )

#     ENTRY_SCHEMA = (
#         StructType()
#         .add("entry", ArrayType(
#              StructType()
#               .add("resource", StructType().add("resourceType", StringType()))
#               .add("fullUrl", StringType())
#          ))
#     )

#     ### Note:  Extends the BundleFhirResource class to add the ability to read and carry over additional metadata from the streaming bronze table.  
#     def read_bundle_data(self, schemas = FhirSchemaModel()) -> DataFrame:
#         return (
#             self._raw_data
#             .withColumn("bundle", from_json("resource", BundleFhirResource.BUNDLE_SCHEMA))
#             .withColumn("entry", from_json("resource", StreamingBundleFhirResource.ENTRY_SCHEMA))
#             .withColumn("entry_struct", arrays_zip("entry.entry.fullUrl", "entry.entry.resource.resourceType")) #root level schema
#             .select(BundleFhirResource.list_entry_columns(schemas ) #entry[] into indvl cols
#                 + [
#                     col("bundle.timestamp"), col("bundle.id"), col("fileMetadata"), col("ingestDate"), col("ingestTime")
#                    ,col("entry_struct")
#                 ] # and root cols timestamp & id, plus ingest metadata
#             ).withColumn("bundleUUID", expr("uuid()"))
#         )
        
    # def fhir_entry(
    #     self
    #     ,bronze_table: str
    #     ,fhir_resource: str
    #     ,live: bool = True
    #     ,temporary: bool = True
    #     ,table_properties: dict = {
    #         "pipelines.autoOptimize.managed" : "true"
    #         ,"pipelines.reset.allowed" : "true"
    #     }):
    #     @dlt.table(
    #         name = f"{fhir_resource}_entry".lower()
    #         ,comment = f"FHIR bundle '{fhir_resource}' entry transformations on streaming FHIR data from bronze. Normally temporary."
    #         ,temporary = temporary
    #         ,table_properties = table_properties
    #     )
    #     def bundle_entry():
    #         fhir_custom = FhirSchemaModel().custom_fhir_resource_mapping([fhir_resource])
    #         if live:
    #             sdf = self.spark.readStream.table(f"LIVE.{bronze_table}")
    #         else:
    #             sdf = self.spark.readStream.table(f"{bronze_table}")
    #         bundle = StreamingFhir.from_raw_bundle_resource(sdf)
    #         return bundle.entry(fhir_custom)

    # def stage_silver(
    #     self
    #     ,entry_table: str
    #     ,fhir_resource: str
    #     ,live: bool = True
    #     ,temporary: bool = True
    #     ):
    #     @dlt.table(
    #         name = f"{fhir_resource}_stage".lower()
    #         ,comment = f"Staging Table for the latest streamed '{fhir_resource}' FHIR resource data recieved in bronze.  Data is staged here to prepare it for upserts into final silver tables.  Normally temporary."
    #         ,temporary = temporary
    #         ,table_properties = {
    #             "pipelines.autoOptimize.managed" : "true"
    #             ,"pipelines.reset.allowed" : "true"
    #         }
    #     )
    #     def stage_silver_fhir():
    #         if live:
    #             sdf = self.spark.readStream.table(f"LIVE.{entry_table}")
    #         else:
    #             sdf = self.spark.readStream.table(f"{entry_table}")
    #         return (
    #             sdf
    #             .withColumn(fhir_resource, explode(fhir_resource).alias(fhir_resource))
    #             .withColumn("bundle_id", col("id"))
    #             .select(col("bundle_id"), col("timestamp"), col("bundleUUID"), col("fileMetadata"), col("ingestDate"), col("ingestTime")
    #                     , col("entry_struct")
    #                     , col(f"{fhir_resource}.*"))
    #             .withColumnRenamed("id", f"{fhir_resource}_id".lower())
    #             .withColumn(f"{fhir_resource}_uuid".lower(), expr(f"filter(entry_struct, x -> x.resourceType == '{fhir_resource}')[0].fullUrl"))
    #         )
