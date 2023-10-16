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

#Tabla plan

plan0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_plan_glue_tb/",
    data_type= "hudi",
    table_name= "plan",
)

plan = plan0.toDF()
plan = plan.select('id',
                func.col('codigo').alias('id_service'),
                func.col('title').alias('ds_service_name'),
                func.col('description').alias('tx_service_description'),
                func.col('price').alias('nm_service_price'),
                func.col('etiqueta_vigencia_servicio').alias('ds_service_validity'),
                func.col('descripcion_superior_detalle').alias('tx_service_detailed_descripcion'),
                func.col('caracteristicas_relevantes').alias('tx_service_relevant_characteristics'),
                func.col('terminos_detalle').alias('tx_service_terms_conditions'),
                func.col('created_date').alias('dt_creation_date'),
                func.col('updated_date').alias('dt_modification_date'),
                func.col('activo').alias('ds_service_status'),
                func.col('months').cast('String').alias('nm_time_validity'),
                func.col('unidad_vigencia').alias('ds_unit_time_validity'),
                'carrito_id')

plan = plan.withColumn('ds_service_type', func.lit('Plan'))
plan = plan.distinct()

#Tabla productos_carrito

producto_carrito0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_producto_carrito_glue_tb/",
    data_type= "hudi",
    table_name= "producto_carrito",
)

producto_carrito = producto_carrito0.toDF()
producto_carrito = producto_carrito.select('id',
                                        func.col('codigo').alias('id_service'),
                                        func.col('nombre').alias('ds_service_name'),
                                        func.col('resumen').alias('tx_service_description'),
                                        func.col('precio').alias('nm_service_price'),
                                        func.col('etiqueta_vigencia_servicio').alias('ds_service_validity'),
                                        func.col('descripcion_superior_detalle').alias('tx_service_detailed_descripcion'),
                                        func.col('caracteristicas_relevantes').alias('tx_service_relevant_characteristics'),
                                        func.col('terminos_detalle').alias('tx_service_terms_conditions'),
                                        func.col('fecha_creacion').alias('dt_creation_date'),
                                        func.col('fecha_actualizacion').alias('dt_modification_date'),
                                        func.col('activo').alias('ds_service_status'),
                                        'carrito_id')

producto_carrito = producto_carrito.withColumn('nm_time_validity', func.lit('')).withColumn('ds_unit_time_validity', func.lit('')).withColumn('ds_service_type', func.lit('Producto'))

producto_carrito = producto_carrito.distinct()


#Tabla portafolio_productos_usuario

portafolio_productos_usuario_plan0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_portafolio_productos_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "portafolio_productos_usuario",
)

portafolio_productos_usuario_plan = portafolio_productos_usuario_plan0.toDF()
portafolio_productos_usuario_plan = portafolio_productos_usuario_plan.select(func.col('id').alias('id_ppu'),
                                                                        func.col('usuario_id').alias('id_user_app_identification_number'),
                                                                        'producto_id',
                                                                        func.col('cantidad_adquirida').alias('nm_services_purchased'),
                                                                        func.col('cantidad_usada').alias('nm_services_used'),
                                                                        'fecha_creacion',
                                                                        'fecha_inicio_vigencia',
                                                                        'fecha_fin_vigencia',
                                                                        'estado_portafolio_producto_id',
                                                                        'tipo_producto_portafolio_id').filter(func.col('tipo_producto_portafolio_id') == 1)

print(f'Conteo portafolio_productos_usuario_plan: {portafolio_productos_usuario_plan.count()}')

portafolio_productos_usuario_producto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_portafolio_productos_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "portafolio_productos_usuario",
)

portafolio_productos_usuario_producto = portafolio_productos_usuario_producto0.toDF()
portafolio_productos_usuario_producto = portafolio_productos_usuario_producto.select(func.col('id').alias('id_ppu'),
                                                                            func.col('usuario_id').cast('String').alias('id_user_app_identification_number'),
                                                                            'producto_id',
                                                                            func.col('cantidad_adquirida').alias('nm_services_purchased'),
                                                                            func.col('cantidad_usada').alias('nm_services_used'),
                                                                            'fecha_creacion',
                                                                            'fecha_inicio_vigencia',
                                                                            'fecha_fin_vigencia',
                                                                            'estado_portafolio_producto_id',
                                                                            'tipo_producto_portafolio_id').filter(func.col('tipo_producto_portafolio_id') == 2)

print(f'Conteo portafolio_productos_usuario_producto: {portafolio_productos_usuario_producto.count()}')

#Tabla portafolio_productos_usuario

estado_portafolio_producto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_estado_portafolio_producto_glue_tb/",
    data_type= "hudi",
    table_name= "estado_portafolio_producto",
)

estado_portafolio_producto = estado_portafolio_producto0.toDF()
estado_portafolio_producto = estado_portafolio_producto.select(func.col('id').alias('id_estado'),func.col('nombre').alias('nombre_estado'))

#Tabla catalogo

servicios = plan.union(producto_carrito)
servicios = servicios.withColumn("ds_service_status", func.expr("CASE WHEN ds_service_status = '0' THEN 'Inactivo' " +
                                                  "WHEN ds_service_status = '1' THEN 'Activo' " +
                                                  "ELSE 'No' END"))
servicios = servicios.na.drop(subset=['id_service'])
servicios = servicios.distinct()
print(f'Conteo catalogo servicios: {servicios.count()}')

servicios = servicios.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())
servicios = servicios.withColumn('dt_creation_date', func.to_timestamp('dt_creation_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
servicios = servicios.withColumn('dt_modification_date', func.to_timestamp('dt_modification_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
servicios = servicios.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
servicios = servicios.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

servicios = servicios.select('id_service',
                            func.col('ds_service_type').alias('ds_type'),
                            func.col('ds_service_name').alias('ds_name'),
                            func.col('tx_service_description').alias('tx_description'),
                            func.col('nm_service_price').alias('nm_price'),
                            func.col('nm_time_validity').cast('Integer').alias('nm_validity_period'),
                            func.col('ds_unit_time_validity').alias('ds_unit_validity_period'),
                            func.col('tx_service_terms_conditions').alias('tx_terms_conditions'),
                            func.col('tx_service_detailed_descripcion').alias('tx_detailed_description'),
                            func.col('tx_service_relevant_characteristics').alias('tx_relevant_characteristics'),
                            func.col('ds_service_status').alias('ds_status'),
                            'dt_creation_date',
                            'dt_modification_date',
                            'dt_product_date_time',
                            'dt_product_hudi_date_time')

ciencuadras_products_service_catalog_glue_tb = DynamicFrame.fromDF(servicios, glueContext, "ciencuadras_products_service_catalog_glue_tb")

ciencuadras_products_service_catalog_glue_tb.show(1)
print(f'Table ciencuadras_products_service_catalog_glue_tb record count: {ciencuadras_products_service_catalog_glue_tb.count()}')

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_service_catalog_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_service_catalog_glue_tb",
    record_id = 'id_service',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}service_catalog{suffixTable}/",
)

#Joins

portafolio_productos_usuario_plan = portafolio_productos_usuario_plan.join(estado_portafolio_producto, 
                                                                           portafolio_productos_usuario_plan.estado_portafolio_producto_id==estado_portafolio_producto.id_estado,
                                                                           'inner').drop('estado_portafolio_producto_id','id_estado')
portafolio_productos_usuario_producto = portafolio_productos_usuario_producto.join(estado_portafolio_producto, 
                                                                                   portafolio_productos_usuario_producto.estado_portafolio_producto_id==estado_portafolio_producto.id_estado,
                                                                                   'inner').drop('estado_portafolio_producto_id','id_estado')

portafolio_plan = portafolio_productos_usuario_plan.join(plan, portafolio_productos_usuario_plan.producto_id==plan.id, 'inner')
portafolio_plan = portafolio_plan.distinct()
portafolio_producto = portafolio_productos_usuario_producto.join(producto_carrito, portafolio_productos_usuario_producto.producto_id==producto_carrito.id, 'inner')
portafolio_producto = portafolio_producto.distinct()

portafolio = portafolio_plan.union(portafolio_producto)
portafolio = portafolio.distinct()
print(f'Count portafolio after union operation: {portafolio.count()}')

portafolio = portafolio.na.drop(subset=['id_ppu','id_user_app_identification_number','id_service'])

portafolio = portafolio.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())
portafolio = portafolio.withColumn('dt_creation_date', func.to_timestamp('dt_creation_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

portafolio = portafolio.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
portafolio = portafolio.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))

portafolio = portafolio.select(func.col('id_ppu').cast('String').alias('id_service_purchased'),
                               'id_user_app_identification_number',
                               'id_service',
                               'ds_service_type',
                               func.col('fecha_inicio_vigencia').alias('dt_inception_date'),
                               func.col('fecha_fin_vigencia').alias('dt_expiration_date'),
                               func.col('nm_services_purchased').alias('nm_purchased_services'),
                               func.col('nm_services_used').alias('nm_services_used'),
                               func.col('nombre_estado').alias('ds_status'),
                               'dt_creation_date',
                               'dt_product_date_time',
                               'dt_product_hudi_date_time')


ciencuadras_products_services_per_user_glue_tb = DynamicFrame.fromDF(portafolio, glueContext, "ciencuadras_products_properties_and_projects_glue_tb")
                    
ciencuadras_products_services_per_user_glue_tb.show(1)
print(f'Table ciencuadras_products_services_per_user_glue_tb record count: {ciencuadras_products_services_per_user_glue_tb.count()}')

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_services_per_user_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_services_per_user_glue_tb",
    record_id = 'id_service_purchased',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}services_per_user{suffixTable}/",
)