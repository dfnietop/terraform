import sys
import boto3

from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as func

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

#Tabla ofertas

ofertas0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_ofertas_glue_tb/",
    data_type= "hudi",
    table_name= "ofertas",
)

ofertas = ofertas0.toDF()
ofertas = ofertas.select(func.col('id').cast('String').alias('id_offer'),
                        func.col('comprador_id').cast('String').alias('id_buyer_user_app'),
                        func.col('vendedor_id').cast('String').alias('id_seller_user_app'),
                        'inmueble_id',
                        func.col('valor').alias('nm_offer_price'),
                        func.col('estado').alias('ds_status'),
                        func.col('tipo').alias('ds_type'),
                        func.col('motivo_rechazo').alias('ds_rejection_cause'),
                        func.col('fecha_creacion').alias('dt_creation_date'),
                        func.col('datos_json').alias('ds_url'),
                        'tipo_rechazo_id')

#Tabla ofertas_tipos_rechazo

ofertas_tipos_rechazo0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_ofertas_tipos_rechazo_glue_tb/",
    data_type= "hudi",
    table_name= "ofertas_tipos_rechazo",
)
ofertas_tipos_rechazo = ofertas_tipos_rechazo0.toDF()
ofertas_tipos_rechazo = ofertas_tipos_rechazo.select('id',func.col('opcion').alias('ds_rejection_option'))

ofertas = ofertas.join(ofertas_tipos_rechazo, ofertas_tipos_rechazo.id == ofertas.tipo_rechazo_id, 'left').drop('tipo_rechazo_id')
ofertas = ofertas.distinct()

#Tabla inmueble

inmueble0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_inmueble_glue_tb/",
    data_type= "hudi",
    table_name= "inmueble",
)
inmueble = inmueble0.toDF()
inmueble = inmueble.select(func.col('id'),func.col('codigo').alias('id_property_offer_publication'))
inmueble = inmueble.distinct()

ofertas = ofertas.join(inmueble, inmueble.id == ofertas.inmueble_id, 'inner')

oferta = ofertas.select('id_offer','id_property_offer_publication','id_buyer_user_app','id_seller_user_app','nm_offer_price','ds_status','ds_type','ds_rejection_cause','ds_rejection_option','dt_creation_date')

oferta = oferta.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())
oferta = oferta.withColumn('dt_creation_date', func.to_timestamp('dt_creation_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
oferta = oferta.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
oferta = oferta.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

oferta = oferta.distinct()
oferta = oferta.na.drop(subset=['id_offer','id_property_offer_publication'])

ciencuadras_products_property_offers_glue_tb = DynamicFrame.fromDF(oferta, glueContext, "ciencuadras_products_property_offers_glue_tb")

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_property_offers_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_property_offers_glue_tb",
    record_id = 'id_offer',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}property_offers{suffixTable}/",
)