import sys
import boto3

from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as func
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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

#Tabla usuario

usuario0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "usuario",
)

usuario = usuario0.toDF()
usuario = usuario.select('id','id_depto',
                            'id_ciudad',
                            'id_tipo_usuario',
                            'id_rol',
                            func.initcap('nombre').alias('nombre'),
                            'id_tipo_documento',
                            func.col('cedula').cast('Integer').alias('cedula'),
                            func.col('nit').cast('Integer').alias('nit'),
                            func.initcap('razon_social').alias('razon_social'),
                            func.lower('correo_electronico').alias('correo_electronico'),
                            'direccion',
                            'telefono',
                            'celular',
                            'activo',
                            'fecha_registro',
                            'fecha_modificacion')

usuario = usuario.withColumnRenamed("id","id_app_identification_number").withColumnRenamed("correo_electronico","ds_email").withColumnRenamed("direccion","ds_address") \
.withColumnRenamed("telefono","ds_phone_number").withColumnRenamed("celular","ds_cellphone_number") \
.withColumnRenamed("fecha_registro","dt_creation_date").withColumnRenamed("fecha_modificacion","dt_modification_date")

#Tabla tipo de documento
tipo_documento0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_tipo_documento_glue_tb/",
    data_type= "hudi",
    table_name= "tipo_documento",
)

tipo_documento = tipo_documento0.toDF()
tipo_documento = tipo_documento.select(func.col('id').alias('id_tipo_documento'),func.col('codigo').alias('cd_identification_type'),'descripcion')

#Tabla tipo usuario
tipo_usuario0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_tipo_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "tipo_usuario",
)
tipo_usuario = tipo_usuario0.toDF()

tipo_usuario = tipo_usuario.select(func.col('id').alias('id_tip_usuario'),func.col('valor').alias('ds_type'))

#Tabla rol
rol0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_rol_glue_tb/",
    data_type= "hudi",
    table_name= "rol",
)
rol = rol0.toDF()
rol = rol.select(func.col('id').alias('id_rol'),func.initcap('valor').alias('ds_role'))

#Tabla ciudad
ciudad0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_ciudad_glue_tb/",
    data_type= "hudi",
    table_name= "ciudad",
)
ciudad = ciudad0.toDF()
ciudad = ciudad.select(func.col('id').alias('id_ciudad'),func.col('valor').alias('ds_city_name'),('codigo_dane'),func.length('codigo_dane').alias('length')).filter(func.isnull('estado'))

ciudad = ciudad.withColumn("cd_city_dane_code", 
                             func.expr("CASE WHEN length = 4 THEN concat('0', codigo_dane) " + 
                                       "WHEN length = 4 THEN codigo_dane " +
                                       "ELSE codigo_dane END"))

ciudad = ciudad.select('id_ciudad', 'cd_city_dane_code', 'ds_city_name',func.substring(ciudad.cd_city_dane_code, 1, 2).alias('cd_state_dane_code'))

#Tabla departamento
departamento0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_departamento_glue_tb/",
    data_type= "hudi",
    table_name= "departamento",
)
departamento = departamento0.toDF()
departamento = departamento.select(func.col('id').alias('id_depto'), func.col('valor').alias('ds_state_name'))

#Tabla tipo habeass data
habeas_data0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_habeas_data_glue_tb/",
    data_type= "hudi",
    table_name= "habeas_data",
)
habeas_data = habeas_data0.toDF()
habeas_data = habeas_data.select('id_usuario','acepta')

#Joins
usuario = usuario.join(habeas_data, usuario.id_app_identification_number == habeas_data.id_usuario, "left")

usuario = usuario.withColumn("fl_is_contact_authorization", 
                             func.expr("CASE WHEN acepta = 1 THEN 'Si' " + 
                                       "ELSE 'No' END"))
                                       
usuario = usuario.join(tipo_documento, usuario.id_tipo_documento == tipo_documento.id_tipo_documento, "left")
usuario = usuario.drop('id_tipo_documento','descripcion')

usuario = usuario.join(tipo_usuario, usuario.id_tipo_usuario == tipo_usuario.id_tip_usuario, "left")
usuario = usuario.drop('id_tip_usuario')

usuario = usuario.withColumn("ds_name", 
                             func.expr("CASE WHEN nombre is not null and ds_type = 'Persona Natural' THEN nombre " + 
                                       "WHEN nombre is null and ds_type != 'Persona Natural' THEN razon_social " +
                                       "WHEN razon_social is not null and ds_type != 'Persona Natural' THEN razon_social " +
                                       "ELSE razon_social END"))
                                       
usuario = usuario.join(ciudad, usuario.id_ciudad == ciudad.id_ciudad, "left").drop('id_ciudad')
usuario = usuario.join(departamento, usuario.id_depto == departamento.id_depto, "left").drop('id_depto')
usuario = usuario.join(rol, usuario.id_rol == rol.id_rol, "left").drop('id_rol')

usuario = usuario.withColumn("cd_identification_number", 
                             func.expr("CASE WHEN cedula is not null and ds_type = 'Persona Natural' THEN cedula " + 
                                       "WHEN cedula is null and ds_type != 'Persona Natural' THEN nit " +
                                       "WHEN nit is not null and ds_type != 'Persona Natural' THEN nit " +
                                       "ELSE null END"))
                                       
usuario = usuario.withColumn("ds_status", 
                             func.expr("CASE WHEN activo = '0' THEN 'Activo' " + 
                                       "WHEN activo = '1' THEN 'Inactivo' " +
                                       "ELSE 'Inactivo' END"))

usuario = usuario.drop('id_tipo_usuario','nombre','razon_social','nit','cedula','activo')

usuario = usuario.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())
usuario = usuario.withColumn('dt_creation_date', func.to_timestamp('dt_creation_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
usuario = usuario.withColumn('dt_modification_date', func.to_timestamp('dt_modification_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
usuario = usuario.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
usuario = usuario.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

usuario = usuario.select(func.col('id_app_identification_number').cast('String').alias('id_app_identification_number'), 'ds_email', 'cd_identification_type', 
                        func.col('cd_identification_number').cast('String').alias('cd_identification_number'), 
                        'ds_type', 'ds_name', 'ds_cellphone_number', 'ds_phone_number', 'ds_address', 'cd_city_dane_code',
                        'ds_city_name', 'cd_state_dane_code', 'ds_state_name', 'fl_is_contact_authorization','ds_role', 'ds_status', 
                        'dt_creation_date', 'dt_modification_date', 'dt_product_date_time', 'dt_product_hudi_date_time')
                         
usuario = usuario.distinct()
usuario = usuario.na.drop(subset=["id_app_identification_number","ds_email","cd_city_dane_code","cd_state_dane_code","ds_city_name","ds_state_name","fl_is_contact_authorization","ds_role","ds_status"])

schema = func.StructType([
            StructField('id_app_identification_number', StringType(), False),
            StructField('ds_email', StringType(), False),
            StructField('cd_identification_type', StringType(), True),
            StructField('cd_identification_number', StringType(), True),
            StructField('ds_type', StringType(), False),
            StructField('ds_name', StringType(), True),
            StructField('ds_cellphone_number', StringType(), True),
            StructField('ds_phone_number', StringType(), True),
            StructField('ds_address', StringType(), True),
            StructField('cd_city_dane_code', StringType(), False),
            StructField('ds_city_name', StringType(), False),
            StructField('cd_state_dane_code', StringType(), False),
            StructField('ds_state_name', StringType(), False),
            StructField('fl_is_contact_authorization', StringType(), False),
            StructField('ds_role', StringType(), False),
            StructField('ds_status', StringType(), False),
            StructField('dt_creation_date', TimestampType(), False),
            StructField('dt_modification_date', TimestampType(), True),
            StructField('dt_product_date_time', TimestampType(), True),
            StructField('dt_product_hudi_date_time', TimestampType(), True)
            ])

emptyRDD = spark.sparkContext.emptyRDD()
usuarios = spark.createDataFrame(emptyRDD,schema)            
usuarios = usuarios.union(usuario)

ciencuadras_products_app_users_glue_tb = DynamicFrame.fromDF(usuarios, glueContext, "ciencuadras_products_app_users_glue_tb")

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_app_users_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_app_users_glue_tb",
    record_id = 'id_app_identification_number',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}app_users{suffixTable}/",
)