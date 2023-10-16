import sys
import boto3

from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as func
from pyspark.sql.types import StructField, StringType, TimestampType

import files.glueAssets.libraries.glueLibraryV2 as gl2

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

#Tabla contacto para inmuebles

contacto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_contacto_glue_tb/",
    data_type= "hudi",
    table_name= "contacto",
)
contacto = contacto0.toDF()
contacto = contacto.select(func.col('id').cast('String').alias('id_app_identification_number'),
                          func.initcap('nombre').alias('ds_name'),
                          func.lower('correo_electronico').alias('ds_email'),
                          func.col('celular').alias('ds_cellphone_number'),
                          func.col('mensaje').alias('tx_message'),
                          func.col('fecha').alias('dt_creation_date'),
                          'id_inmueble',
                          func.upper( 'tipo_doc').alias('tipo_doc'),
                          func.col('cedula').alias('cd_identification_number'),
                          'origin_lead','fuente','enviado_desde')

contacto = contacto.withColumn("cd_identification_type", 
                             func.expr("CASE WHEN tipo_doc in ('CÉDULA DE CIUDADANIA','1-CC','01','1','CC - CÉDULA DE CIUDA','CC') THEN 'CC' " + 
                                       "WHEN tipo_doc in ('5-CE','CE','CÉDULA DE EXTRANJERÍ','5','CÉDULA DE EXTRANJERÍA') THEN 'CE' " +
                                       "WHEN tipo_doc in ('2','02','NIT','2-NIT','NIT - Número de iden') THEN 'NIT' " +
                                       "WHEN tipo_doc in ('3','PASAPORTE','PA','3-PA') THEN 'PA' " +
                                       "WHEN tipo_doc in ('4-TI','TI') THEN 'TI' " +
                                       "WHEN tipo_doc in ('SIDE','SIDE - Sin identific') THEN 'SIDE' " +
                                       "WHEN tipo_doc in ('RC') THEN 'RC' " +
                                       "WHEN tipo_doc in ('PEP') THEN 'PEP' " +  
                                       "WHEN tipo_doc in ('TE','TE - Tarjeta de extr') THEN 'TE' " +
                                       "WHEN tipo_doc in ('TDE') THEN 'TDE' " +
                                       "ELSE tipo_doc END"))

#Tabla inmueble

inmueble0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_inmueble_glue_tb/",
    data_type= "hudi",
    table_name= "inmueble",
)
inmueble = inmueble0.toDF()
inmueble = inmueble.select(func.col('id'),func.col('codigo').alias('id_publication'),'id_usuario')

#Tabla usuario

usuario0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "usuario",
)

usuario = usuario0.toDF()
usuario = usuario.select(func.col('id').alias('id_user_app_identification_number'),'activo')

# Joins

contacto = contacto.join(inmueble, contacto.id_inmueble == inmueble.id, 'inner')

contacto = contacto.join(usuario, contacto.id_usuario == usuario.id_user_app_identification_number, 'left') 

contacto = contacto.withColumn('ds_source_lead', func.lit('U'))
contacto = contacto.distinct()

contacto = contacto.select(func.concat_ws('-',contacto.id_app_identification_number,contacto.ds_source_lead).alias('id_app_identification_number'),
                            'cd_identification_type','cd_identification_number','ds_email','ds_name','ds_cellphone_number','tx_message',
                            'id_user_app_identification_number','id_publication','dt_creation_date','origin_lead','fuente','enviado_desde')

# Tabla contacto_proyectos

contacto_proyecto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_contacto_proyecto_glue_tb/",
    data_type= "hudi",
    table_name= "contacto_proyecto",
)
contacto_proyecto = contacto_proyecto0.toDF()

contacto_proyecto = contacto_proyecto.select(func.col('id').cast('String').alias('id_app_identification_number'),
                                            'id_proyecto',
                                            func.initcap('nombre').alias('ds_name'),
                                            func.lower('correo_electronico').alias('ds_email'),
                                            func.col('celular').alias('ds_cellphone_number'),
                                            func.col('mensaje').alias('tx_message'),
                                            func.col('fecha').alias('dt_creation_date'),
                                            func.upper('tipo_doc').alias('tipo_doc'),
                                            func.col('cedula').alias('cd_identification_number'),
                                            'origin_lead','fuente','enviado_desde')

contacto_proyecto = contacto_proyecto.withColumn("cd_identification_type", 
                             func.expr("CASE WHEN tipo_doc in ('CÉDULA DE CIUDADANIA','1-CC','01','1','CC - CÉDULA DE CIUDA','CC') THEN 'CC' " + 
                                       "WHEN tipo_doc in ('5-CE','CE','CÉDULA DE EXTRANJERÍ','5','CÉDULA DE EXTRANJERÍA') THEN 'CE' " +
                                       "WHEN tipo_doc in ('2','02','NIT','2-NIT','NIT - Número de iden') THEN 'NIT' " +
                                       "WHEN tipo_doc in ('3','PASAPORTE','PA','3-PA') THEN 'PA' " +
                                       "WHEN tipo_doc in ('4-TI','TI') THEN 'TI' " +
                                       "WHEN tipo_doc in ('SIDE','SIDE - Sin identific') THEN 'SIDE' " +
                                       "WHEN tipo_doc in ('RC') THEN 'RC' " +
                                       "WHEN tipo_doc in ('PEP') THEN 'PEP' " +  
                                       "WHEN tipo_doc in ('TE','TE - Tarjeta de extr') THEN 'TE' " +
                                       "WHEN tipo_doc in ('TDE') THEN 'TDE' " +
                                       "ELSE tipo_doc END"))

#Tabla inmuebles_tipo

inmuebles_tipo0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_inmuebles_tipo_glue_tb/",
    data_type= "hudi",
    table_name= "inmuebles_tipo",
)

inmuebles_tipo = inmuebles_tipo0.toDF()
inmuebles_tipo = inmuebles_tipo.select('id_inmueble_tipo','id_proyecto',func.col('codigo_tipo_propiedad').alias('id_property_project_building_class'))
inmuebles_tipo = inmuebles_tipo.withColumn('id_tipologia', func.split(inmuebles_tipo['id_property_project_building_class'], '-').getItem(1))

#Tabla proyecto

proyecto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_proyectos_glue_tb/",
    data_type= "hudi",
    table_name= "proyectos",
)

proyecto = proyecto0.toDF()
proyecto = proyecto.select(func.col('id_proyecto').alias('id'),func.col('codigo_proyecto').alias('id_property_project'),'id_usuario')

proyecto = proyecto.join(inmuebles_tipo, proyecto.id == inmuebles_tipo.id_proyecto, 'inner').drop('id_proyecto')
proyecto = proyecto.select('*',func.concat_ws('-',proyecto.id_property_project,proyecto.id_tipologia).alias('id_publication')) 

contacto_proyecto = contacto_proyecto.join(proyecto, contacto_proyecto.id_proyecto == proyecto.id, 'inner')
contacto_proyecto = contacto_proyecto.join(usuario, contacto_proyecto.id_usuario == usuario.id_user_app_identification_number, 'left')

contacto_proyecto = contacto_proyecto.withColumn('ds_source_lead', func.lit('N'))
contacto_proyecto = contacto_proyecto.distinct()

contacto_proyecto = contacto_proyecto.select(func.concat_ws('-',contacto_proyecto.id_app_identification_number,contacto_proyecto.ds_source_lead).alias('id_app_identification_number'),
                                            'cd_identification_type','cd_identification_number','ds_email','ds_name','ds_cellphone_number','tx_message',
                                            'id_user_app_identification_number','id_publication','dt_creation_date','origin_lead','fuente','enviado_desde')

contactos = contacto.union(contacto_proyecto)

contactos = contactos.withColumn("fl_is_registered_user", 
                             func.expr("CASE WHEN id_user_app_identification_number is not null or ds_email is not null THEN 'Si' " + 
                                       "ELSE 'No' END"))

contactos = contactos.distinct()
contactos = contactos.na.drop(subset=['id_app_identification_number'])

contactos = contactos.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())

contactos = contactos.select(func.col('id_app_identification_number').cast('String').alias('id_app_identification_number'),
                            'cd_identification_type',
                            'cd_identification_number',
                            'ds_email',
                            'ds_name',
                            'ds_cellphone_number',
                            'tx_message',
                            func.col('id_user_app_identification_number').cast('String').alias('id_user_app_identification_number'),
                            'id_publication',
                            'fl_is_registered_user',
                            'dt_creation_date',
                            'dt_product_date_time',
                            'dt_product_hudi_date_time')

schema = func.StructType([
            StructField('id_app_identification_number', StringType(), False),
            StructField('cd_identification_type', StringType(), True),
            StructField('cd_identification_number', StringType(), True),
            StructField('ds_email', StringType(), False),
            StructField('ds_name', StringType(), False),
            StructField('ds_cellphone_number', StringType(), False),
            StructField('tx_message', StringType(), True),
            StructField('id_user_app_identification_number', StringType(), True),
            StructField('id_publication', StringType(), True),
            StructField('fl_is_registered_user', StringType(), True),
            StructField('dt_creation_date', TimestampType(), False),
            StructField('dt_product_date_time', TimestampType(), False),
            StructField('dt_product_hudi_date_time', TimestampType(), False)
            ])

emptyRDD = spark.sparkContext.emptyRDD()
leads = spark.createDataFrame(emptyRDD,schema)            
leads = leads.union(contactos)

ciencuadras_products_leads_glue_tb = DynamicFrame.fromDF(leads, glueContext, "ciencuadras_products_leads_glue_tb")
                    
gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_leads_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_leads_glue_tb",
    record_id = 'id_app_identification_number',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}leads{suffixTable}/",
)