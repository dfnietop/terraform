from pyspark.sql.types import StringType
import sys
import os
import json
import datetime

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, dense_rank, desc, when, trim
from pyspark.sql.window import Window

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

import glueLibraryV2 as gl2

args = getResolvedOptions(sys.argv, ['JOB_NAME','BUCKET_ORIG','BUCKET_DEST','BUCKET_CONF','DB_NAME', 'ROUTE','FORMAT','PREFIX_TABLE_DEST','SUFIX_TABLE_DEST'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer')\
    .config('spark.sql.hive.convertMetastoreParquet', 'false')\
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")\
    .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED")\
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

route = args['ROUTE']
hudiStorageType = 'CoW'
dropColumnList = ['db','table_name','Op']
dbName = args['DB_NAME']
format = args['FORMAT']
sourceBucketName = args['BUCKET_ORIG']
configBucketName = args['BUCKET_CONF']
targetBucketName = args['BUCKET_DEST']
prefixTable = args['PREFIX_TABLE_DEST']
suffixTable = args['SUFIX_TABLE_DEST']

def transformations(df):
    df2 = df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
    df3 = df2.select([(trim(c.name).alias(c.name) if isinstance(c.dataType, StringType) else c.name) for c in df.schema])
    return df3

logger.info('Initialization')
s3 = boto3.resource('s3')

# Get source tables
bucket = s3.Bucket(sourceBucketName)
keys = []
sourceSchemaNames = []
sourceTableNames = []
for obj in bucket.objects.filter(Prefix=route):
    key = obj.key[0: obj.key.rfind('/')+1]
    if key not in keys:
        keys.append(key)
        sourceSchemaNames.append(key.split('/')[1])
        sourceTableNames.append(key.split('/')[2])

for sourceSchemaName, sourceTableName in zip(sourceSchemaNames,sourceTableNames):
    
    targetTableName = f"{prefixTable}{sourceTableName}{suffixTable}"
    sourceS3TablePath = f's3://{sourceBucketName}/{route}/{sourceSchemaName}/{sourceTableName}/'
    targetS3TablePath = f's3://{targetBucketName}/{route}/{dbName}/{targetTableName}'
    
    # 1. read data from origin
    logger.info(f'read table {sourceSchemaName}.{sourceTableName} from s3 path {sourceS3TablePath}')
    dyf = gl2.read_data_2(
        spark,
        glueContext= glueContext,
        s3_url= sourceS3TablePath,
        data_type= "parquet",
        table_name= sourceTableName,
        ingestion_type="cdc"
    )
    
    # 2. load mapping configs
    logger.info(f'load mapping configs {sourceSchemaName}.{sourceTableName} from s3 path s3://{configBucketName}/mappings/mapping_{sourceTableName}.json')
    table_config = gl2.load_mapping_config(
        configBucketName,
        f"mappings/mapping_{sourceTableName}.json"
    )
    
    primary_key = table_config["primary_key"]
        
    # 3. apply transformations using pyspark
    logger.info(f'Apply transformations {sourceSchemaName}.{sourceTableName}')
    # dyf.withColumn(...)
    df = dyf.toDF()
    df2 = df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
    df3 = df2.select([(trim(c.name).alias(c.name) if isinstance(c.dataType, StringType) else c.name) for c in df.schema])
    
    dyf2 = DynamicFrame.fromDF(df3, glueContext, targetTableName)
    
    # 4. upsert hudi table. 
    logger.info(f'upserting hudi table {dbName}.{targetTableName} in s3 path {targetS3TablePath}')
    gl2.upsert_hudi_table(
        spark_dyf = dyf2,
        glue_database = dbName,
        table_name = targetTableName,
        record_id = primary_key,
        precomb_key = "field_timestamp",
        target_path = targetS3TablePath,
        ingestion_type = "cdc"
    )

job.commit()