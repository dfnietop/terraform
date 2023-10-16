import sys
import boto3

from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType, FloatType

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

#Tabla tipo de documento

tipo_documento0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_tipo_documento_glue_tb/",
    data_type= "hudi",
    table_name= "tipo_documento",
)

tipo_documento = tipo_documento0.toDF()
tipo_documento = tipo_documento.select('id',func.col('codigo').alias('cd_identification_type'),'descripcion')

#Tabla ciencuadras_curated_cliente_glue_tb

cliente0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_cliente_glue_tb/",
    data_type= "hudi",
    table_name= "cliente",
)
cliente = cliente0.toDF()
cliente = cliente.select('id',func.col('nombre').alias('ds_name'),func.col('numero_documento').cast('String').alias('cd_identification_number'),
                        func.col('nombre_formulario').alias('ds_form'),'id_tipo_documento','habeas_data')

#Tabla ciencuadras_curated_correo_contacto_cliente_glue_tb

correo_contacto_cliente0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_correo_contacto_cliente_glue_tb/",
    data_type= "hudi",
    table_name= "correo_contacto_cliente",
)
correo_contacto_cliente = correo_contacto_cliente0.toDF()
correo_contacto_cliente = correo_contacto_cliente.select('id_cliente',func.col('correo_electronico').alias('ds_email'))

#Tabla perfilamiento_cliente

perfilamiento_cliente0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_perfilamiento_cliente_glue_tb/",
    data_type= "hudi",
    table_name= "perfilamiento_cliente",
)
perfilamiento_cliente = perfilamiento_cliente0.toDF()

perfilamiento_cliente = perfilamiento_cliente.select(func.col('id').cast('String').alias('id_profile'),
                                                'id_registro_cliente',
                                                'id_tipo_transaccion',
                                                func.col('perfilamiento').alias('tx_profile'),
                                                'fecha_creacion',
                                                'fecha_modificacion')

perfilamiento_cliente = perfilamiento_cliente.withColumn("ds_transaction_type", 
                             func.expr("CASE WHEN id_tipo_transaccion = 1 THEN 'Compra' " + 
                                       "WHEN id_tipo_transaccion = 2 THEN 'Arriendo' " +
                                       "ELSE '' END")).drop('id_tipo_transaccion')

perfilamiento_cliente = perfilamiento_cliente.join(cliente, perfilamiento_cliente.id_registro_cliente == cliente.id, 'left').drop('id_registro_cliente')
perfilamiento_cliente = perfilamiento_cliente.distinct()

perfilamiento_cliente = perfilamiento_cliente.withColumn("fl_is_contact_authorization", 
                             func.expr("CASE WHEN habeas_data = 1 THEN 'Si' " + 
                                       "ELSE 'No' END")).drop('habeas_data')

perfilamiento_cliente = perfilamiento_cliente.join(correo_contacto_cliente, perfilamiento_cliente.id == correo_contacto_cliente.id_cliente, 'left').drop('id','id_cliente')
perfilamiento_cliente = perfilamiento_cliente.distinct()

perfilamiento_cliente = perfilamiento_cliente.join(tipo_documento, perfilamiento_cliente.id_tipo_documento == tipo_documento.id, 'left')
perfilamiento_cliente = perfilamiento_cliente.distinct()

perfilamiento_cliente = perfilamiento_cliente.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())

perfilamiento_cliente = perfilamiento_cliente.withColumn('dt_creation_date', func.to_timestamp('fecha_creacion', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
perfilamiento_cliente = perfilamiento_cliente.withColumn('dt_modification_date', func.to_timestamp('fecha_modificacion', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

perfilamiento_cliente = perfilamiento_cliente.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
perfilamiento_cliente = perfilamiento_cliente.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

perfilamiento_cliente = perfilamiento_cliente.select('id_profile','ds_email','cd_identification_type','cd_identification_number',
                                                    'ds_name','ds_transaction_type','tx_profile','fl_is_contact_authorization','ds_form',
                                                    'dt_creation_date','dt_modification_date','dt_product_date_time','dt_product_hudi_date_time')

perfilamiento_cliente = perfilamiento_cliente.distinct()

ciencuadras_products_customer_profile_glue_tb = DynamicFrame.fromDF(perfilamiento_cliente, glueContext, "ciencuadras_products_customer_profile_glue_tb")
                    
ciencuadras_products_customer_profile_glue_tb.show(1)

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_customer_profile_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_customer_profile_glue_tb",
    record_id = 'id_profile',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}customer_profile{suffixTable}/",
)