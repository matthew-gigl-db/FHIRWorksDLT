import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
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
        ,meta_keys: list
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

            return (
                sdf
                .groupBy(*grouping_cols)
                .agg(
                    *[element_at(
                        collect_list(when(col("key") == k, col("value"))), 1
                    ).alias(k) for k in meta_keys]
                )
            )

    def resource_stage_silver(
        self
        ,parsed_variant_resource_table: str
        ,resource_keys: list
        ,resource: str
        ,live: bool = True
        ,temporary: bool = True
        ):
        @dlt.table(
            name = f"{resource}_stage".lower()
            ,comment = f"Staging Table for the latest streamed bundle {resource} data from the FHIR resource data recieved in bronze and variant exploded. This stage table transposes the exploded variant data wide for easier access.  Normally temporary."
            ,temporary = temporary
            ,table_properties = {
                "pipelines.autoOptimize.managed" : "true"
                ,"pipelines.reset.allowed" : "true"
                ,"delta.feature.variantType-preview" : "supported"
            }
        )
        def stage_resource():
            if live:
                src_tbl_name = f"LIVE.{parsed_variant_resource_table}"
            else:
                src_tbl_name = f"{parsed_variant_resource_table}"

            sdf = self.spark.readStream.table(src_tbl_name).filter(col("resourceType") == resource).withColumnRenamed("fullUrl", f"{resource}_uuid".lower())
            grouping_cols = [col for col in sdf.columns if col not in ["pos", "key", "value", "fileMetadata", "ingestDate", "ingestTime", "bundle_id", "resourceType"]]

            return (
                sdf
                .groupBy(*grouping_cols)
                .agg(
                    *[element_at(
                        collect_list(when(col("key") == k, col("value"))), 1
                    ).alias(k) for k in resource_keys]
                )
            )

    ## stream changes into target silver table
    def stream_silver_apply_changes(
        self
        ,source: str
        ,target: str
        ,keys:  list
        ,sequence_by: str
        ,stored_as_scd_type: int = 1
        ,comment: str = None
        ,spark_conf: dict = None
        ,table_properties: dict = None
        ,partition_cols: list = None
        ,cluster_by: list = None
        ,path: str = None
        ,schema: str = None
        ,expect_all: dict = None
        ,expect_all_or_drop: dict = None
        ,expect_all_or_fail: dict = None
        ,row_filter: str = None
        ,ignore_null_updates: bool = False
        ,apply_as_deletes: str = None
        ,apply_as_truncates: str = None
        ,column_list: list = None
        ,except_column_list: list = None
        ,track_history_column_list: list = None
        ,track_history_except_column_list: list = None
    ):
        # create the target table
        dlt.create_streaming_table(
            name = target
            ,comment = comment
            ,spark_conf=spark_conf
            ,table_properties=table_properties
            ,partition_cols=partition_cols
            ,cluster_by = cluster_by
            ,path=path
            ,schema=schema
            ,expect_all = expect_all
            ,expect_all_or_drop = expect_all_or_drop
            ,expect_all_or_fail = expect_all_or_fail
            ,row_filter = row_filter
        )

        dlt.apply_changes(
            target = target
            ,source = source
            ,keys = keys
            ,sequence_by = sequence_by
            ,ignore_null_updates = ignore_null_updates
            ,apply_as_deletes = apply_as_deletes
            ,apply_as_truncates = apply_as_truncates
            ,column_list = column_list
            ,except_column_list = except_column_list
            ,stored_as_scd_type = stored_as_scd_type
            ,track_history_column_list = track_history_column_list
            ,track_history_except_column_list = track_history_except_column_list
        )

    
