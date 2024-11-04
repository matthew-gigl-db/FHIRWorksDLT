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

# read streaming data as whole text using autoloader    
def read_stream_raw(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, wholeText: bool = False, skipRows: int = 0, options: dict = None) -> DataFrame:
    stream_schema = "value VARIANT"
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

class fhirIngestionDLT:

    def __init__(
        self
        ,spark: SparkSession # = spark
        ,volume: str
    ):
        self.spark = spark
        self.volume = volume

    def __repr__(self):
        return f"""fhirIngestionDLT(volume='{self.volume}')"""

    def ingest_raw_to_bronze(self, table_name: str, table_comment: str, table_properties: dict, source_folder_path_from_volume: str = "", maxFiles: int = 1000, maxBytes: str = "10g", options: dict = None):
        """
        Ingests all files in a volume's path to a key value pair bronze table.
        """
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
                .withColumn("response", col("value"))
                .select(
                    "fullFilePath"
                    ,lit(file_path).alias("datasource")
                    ,"inputFileName"
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                    ,"response"
                    ,"fileMetadata"
                )
            )

            return bronze_df