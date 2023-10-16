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

#Tabla ciudad

ciudad0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_ciudad_glue_tb/",
    data_type= "hudi",
    table_name= "ciudad",
)
ciudad = ciudad0.toDF()
ciudad = ciudad.select(func.col('id').alias('id_ciudad'),func.col('valor').alias('ds_city_name'),('codigo_dane'),func.length('codigo_dane').alias('length'))

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
departamento = departamento.select(func.col('id').alias('id_departamento'), func.col('valor').alias('ds_state_name'))

#Tabla localidad

localidad0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_localidad_glue_tb/",
    data_type= "hudi",
    table_name= "localidad",
)
localidad = localidad0.toDF()
localidad = localidad.select(func.col('id').alias('id_localidad'),func.initcap('valor').alias('ds_district_division'))

#Tabla barrio

barrio0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_barrio_glue_tb/",
    data_type= "hudi",
    table_name= "barrio",
)
barrio = barrio0.toDF()
barrio = barrio.select(func.col('id').alias('id_barrio'),func.initcap('valor').alias('ds_neighborhood'))

#Tabla s_estrato

estrato0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_s_estrato_glue_tb/",
    data_type= "hudi",
    table_name= "s_estrato",
)
estrato = estrato0.toDF()
estrato = estrato.select(func.col('id').alias('id_estrato'),func.col('valor').alias('cd_neighborhood_economic_level'))

#Tabla usuario

usuario0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_usuario_glue_tb/",
    data_type= "hudi",
    table_name= "usuario",
)

usuario = usuario0.toDF()
usuario = usuario.select(func.col('id').alias('id_publication_owner_app_identification_number'),func.lower('correo_electronico').alias('ds_publication_owner_email'),'activo','nombre')

#Tabla tipo inmueble

tipo_inmueble0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_s_tipo_inmueble_glue_tb/",
    data_type= "hudi",
    table_name= "s_tipo_inmueble",
)

tipo_inmueble = tipo_inmueble0.toDF()
tipo_inmueble = tipo_inmueble.select(func.col('id').alias('id_tipo_inmueble'),func.col('valor').alias('ds_property_type'))

#Tabla s_estado_proyecto

s_estado_proyecto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_s_estado_proyecto_glue_tb/",
    data_type= "hudi",
    table_name= "s_estado_proyecto",
)

s_estado_proyecto = s_estado_proyecto0.toDF()
s_estado_proyecto = s_estado_proyecto.select(func.col('id').alias('id_estado_proyecto'),func.col('valor').alias('ds_property_project_status'))

#Tabla detalle_caracteristicas_inmueble

detalle_caracteristicas_inmueble0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_detalle_caracteristicas_inmueble_glue_tb/",
    data_type= "hudi",
    table_name= "detalle_caracteristicas_inmueble",
)

detalle_caracteristicas_inmueble = detalle_caracteristicas_inmueble0.toDF()
detalle_caracteristicas_inmueble = detalle_caracteristicas_inmueble.select('id_inmueble','id_caracteristica','valor')

#Tabla definicion_caracteristicas

definicion_caracteristicas0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_definicion_caracteristicas_glue_tb/",
    data_type= "hudi",
    table_name= "definicion_caracteristicas",
)

definicion_caracteristicas = definicion_caracteristicas0.toDF()
definicion_caracteristicas = definicion_caracteristicas.select('id_caracteristica','alias')

# Joins

definicion_caracteristicas = detalle_caracteristicas_inmueble.join(definicion_caracteristicas, detalle_caracteristicas_inmueble.id_caracteristica == definicion_caracteristicas.id_caracteristica, 'inner')

caracteristicas_nuevos = definicion_caracteristicas.groupBy('id_inmueble').pivot('alias').agg(func.first('valor').alias('valor')) 

#Tabla inmuebles_tipo

inmuebles_tipo0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_inmuebles_tipo_glue_tb/",
    data_type= "hudi",
    table_name= "inmuebles_tipo",
)

inmuebles_tipo = inmuebles_tipo0.toDF()
inmuebles_tipo = inmuebles_tipo.select('id_inmueble_tipo','id_proyecto','estado',func.col('codigo_tipo_propiedad').alias('id_property_project_building_class'),
                                        func.col('nombre').alias('ds_property_project_building_class_name'))
inmuebles_tipo = inmuebles_tipo.withColumn('id_tipologia', func.split(inmuebles_tipo['id_property_project_building_class'], '-').getItem(1))


inmuebles_tipo = inmuebles_tipo.withColumn("ds_property_project_building_class_status", 
                                           func.expr("CASE WHEN estado = 'A' THEN 'Activo' " + 
                                                    "WHEN estado = 'I' THEN 'Inactivo' " +
                                                    "ELSE 'Eliminado' END")).drop('estado')

inmuebles_tipo = inmuebles_tipo.join(caracteristicas_nuevos, inmuebles_tipo.id_inmueble_tipo == caracteristicas_nuevos.id_inmueble, "inner")

#Tabla proyecto

proyecto0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_proyectos_glue_tb/",
    data_type= "hudi",
    table_name= "proyectos",
)

proyecto = proyecto0.toDF()
proyecto = proyecto.select(func.col('id_proyecto').alias('id'),
                          func.col('codigo_proyecto').alias('id_property_project'),
                          'id_ciudad','id_localidad','id_barrio',
                          'id_departamento','id_tipo_inmueble',
                          func.col('direccion').alias('ds_address'),
                          func.col('estrato').alias('id_estrato'),
                          func.col('latitud').cast('String').alias('ds_latitude'),
                          func.col('longitud').cast('String').alias('ds_longitude'),'estado',
                          func.initcap('nombre_proyecto').alias('ds_property_project_name'),
                          'id_usuario',
                          func.col('fecha_creacion').alias('dt_creation_date'),
                          func.col('fecha_modificacion').alias('dt_modification_date'),
                          func.col('fecha_entrega').alias('dt_property_project_delivery_date'),'id_estado_proyecto')

proyecto = proyecto.withColumn("ds_publication_status", 
                                       func.expr("CASE WHEN estado = 'A' THEN 'Activo' " + 
                                                "WHEN estado = 'I' THEN 'Inactivo' " +
                                                "ELSE 'Eliminado' END")).drop('estado')

proyecto = proyecto.join(s_estado_proyecto, proyecto.id_estado_proyecto == s_estado_proyecto.id_estado_proyecto, 'inner')

proyecto = proyecto.join(inmuebles_tipo, proyecto.id == inmuebles_tipo.id_proyecto, "inner")

proyecto = proyecto.select('*',func.concat_ws('-',proyecto.id_property_project,proyecto.id_tipologia).alias('id_publication')) 
proyecto = proyecto.join(usuario, proyecto.id_usuario == usuario.id_publication_owner_app_identification_number, "inner")
proyecto = proyecto.join(estrato, proyecto.id_estrato == estrato.id_estrato, "left").drop('id_estrato')
proyecto = proyecto.join(ciudad, proyecto.id_ciudad == ciudad.id_ciudad, "left").drop('id_ciudad')
proyecto = proyecto.join(departamento, proyecto.id_departamento == departamento.id_departamento, "left").drop('id_departamento')
proyecto = proyecto.join(localidad, proyecto.id_localidad == localidad.id_localidad, "left").drop('id_localidad')
proyecto = proyecto.join(barrio, proyecto.id_barrio == barrio.id_barrio, "left").drop('id_barrio')
proyecto = proyecto.join(tipo_inmueble, proyecto.id_tipo_inmueble == tipo_inmueble.id_tipo_inmueble, "left").drop('id_tipo_inmueble')

proyecto = proyecto.withColumn('ds_publication_type', func.lit('Proyecto Inmobiliario'))\
                  .withColumn('ds_publication_site_store',func.lit('Nuevo'))\
                  .withColumn('nm_property_age',func.lit(None))\
                  .withColumn('nm_property_monthly_rent_payment',func.lit(None))\
                  .withColumn('ds_property_real_state_registration_number',func.lit(None))\
                  .withColumn('fl_property_offer',func.lit('No'))\
                  .withColumn('fl_has_gym',func.lit('No'))\
                  .withColumn('nm_elevator_number',func.lit(None))\
                  .withColumn('nm_visitors_parking_number',func.lit(None))\
                  .withColumn('fl_has_reception',func.lit('No'))\
                  .withColumn('fl_has_social_room',func.lit('No'))\
                  .withColumn('fl_has_communal_living',func.lit('No'))\
                  .withColumn('fl_has_children_zone',func.lit('No'))\
                  .withColumn('fl_has_green_zones',func.lit('No'))\
                  .withColumn('fl_has_vigilance', func.lit('No'))\
                  .withColumn('dt_property_inception_date', func.lit(None))\
                  .withColumn('dt_property_expiration_date', func.lit(None))\
                  .withColumn('dt_property_deletion_date', func.lit(None)) 

proyecto = proyecto.withColumnRenamed("sellingPrice","nm_property_selling_price")\
                  .withColumnRenamed("administrationValue","nm_property_monthly_administration_fee")\
                  .withColumnRenamed("builtArea","nm_built_area")\
                  .withColumnRenamed("privateArea","nm_private_area")\
                  .withColumnRenamed("numParking","nm_parking_number")\
                  .withColumnRenamed("numBedRooms","nm_bedroom_number")\
                  .withColumnRenamed("numBathrooms","nm_bathroom_number")\
                  .withColumnRenamed("balconiesNumber","nm_balcony_number")\
                  .withColumnRenamed("terracesNumber","nm_terrace_number")\
                  .withColumnRenamed("depositsNumber","nm_storage_room_number")\
                  .withColumn('nm_square_meter_price',func.round(func.col('nm_property_selling_price') / func.col('nm_built_area'), 2))
                  
proyecto = proyecto.withColumn("url",
    func.expr("CASE WHEN ds_property_type in ('Casa','Apartaestudio','Apartamento','Finca','Lote') THEN concat('https://www.ciencuadras.com/proyecto-de-vivienda/', replace(trim(lower(nombre)),' ','-'), '-', replace(trim(lower(ds_property_project_name)),' ','-'),'-', replace(trim(lower(ds_city_name)),' ','-'),'-',id ) " +
              "ELSE concat('https://www.ciencuadras.com/proyecto-comercial/', replace(trim(lower(nombre)),' ','-'), '-', replace(trim(lower(ds_property_project_name)),' ','-'),'-', replace(trim(lower(ds_city_name)),' ','-'),'-',id ) END"))

proyecto = proyecto.withColumn("tx_url",func.expr("CASE WHEN url is not null THEN translate(url,'áéíóú','aeiou')" +
                                                    "ELSE 'https://www.ciencuadras.com/' END"))

proyecto = proyecto.distinct()

proyectos = proyecto.select('id_publication','ds_publication_status','ds_publication_type','ds_publication_site_store','id_publication_owner_app_identification_number','ds_publication_owner_email',
                    'ds_property_real_state_registration_number','ds_property_type','id_property_project','ds_property_project_name','ds_property_project_status','id_property_project_building_class',
                    'ds_property_project_building_class_name','ds_property_project_building_class_status','cd_state_dane_code','ds_state_name','cd_city_dane_code','ds_city_name','ds_neighborhood',
                    'ds_district_division','cd_neighborhood_economic_level','ds_address','ds_latitude','ds_longitude',
                    'nm_property_selling_price','nm_property_monthly_rent_payment','nm_property_monthly_administration_fee', func.col('nm_square_meter_price').cast('String').alias('nm_square_meter_price'),
                    'nm_built_area','nm_private_area','nm_parking_number','nm_visitors_parking_number','nm_bedroom_number','nm_bathroom_number','nm_property_age','nm_balcony_number',
                    'nm_terrace_number','nm_storage_room_number','nm_elevator_number','allowPets','laundryZone','fl_has_green_zones','fl_has_communal_living','fl_has_children_zone','privatePool',
                    'fl_has_gym','serviceRoom','serviceBathroom','fl_has_social_room','fl_has_reception','airConditioner','homeAppliances','dt_creation_date','dt_modification_date',
                    'dt_property_inception_date','dt_property_expiration_date','dt_property_deletion_date','dt_property_project_delivery_date','fl_property_offer','tx_url','fl_has_vigilance')

#Tabla inmueble

inmueble0 = gl2.read_data_2(
    spark,
    glueContext= glueContext,
    s3_url= f"s3://{sourceBucketName}/{route}/{dbName}/ciencuadras_curated_inmueble_glue_tb/",
    data_type= "hudi",
    table_name= "inmueble",
)
inmueble = inmueble0.toDF()
inmueble = inmueble.select(func.col('id'),
                          func.col('id_depto').alias('id_departamento'),
                          'id_ciudad','id_localidad','id_barrio','id_tipo_inmueble','id_tipo_transaccion','id_usuario',
                          func.col('direccion').alias('ds_address'),
                          func.col('estrato').alias('id_estrato'),
                          func.col('codigo').alias('id_publication'),
                          func.col('latitud').alias('ds_latitude'),
                          func.col('longitud').alias('ds_longitude'),
                          func.col('precio_venta').cast('String').alias('nm_property_selling_price'),
                          func.col('canon_arrendamiento').cast('String').alias('nm_property_monthly_rent_payment'),
                          func.col('valor_administracion').cast('String').alias('nm_property_monthly_administration_fee'),
                          func.abs('num_parqueaderos').cast('Integer').alias('nm_parking_number'),
                          func.abs('num_habitaciones').cast('Integer').alias('nm_bedroom_number'),
                          func.abs('num_banos').cast('Integer').alias('nm_bathroom_number'),
                          func.abs('area_bodega').alias('area_bodega'),
                          func.abs('area_oficina').alias('area_oficina'),
                          func.abs('area_lote').alias('area_lote'), 
                          func.abs('area_construida').alias('area_construida'),
                          func.abs('area_privada').alias('nm_private_area'),
                          func.abs('antiguedad').cast('Integer').alias('nm_property_age'),
                          func.col('cuarto_servicio').cast('String').alias('serviceRoom'),
                          func.col('bano_servicio').cast('String').alias('serviceBathroom'),
                          func.col('zona_lavanderia').cast('String').alias('laundryZone'),
                          func.col('aire_acondicionado').cast('String').alias('airConditioner'),
                          func.col('electrodomesticos').alias('homeAppliances'),
                          func.abs('num_balcones').cast('Integer').alias('nm_balcony_number'),
                          func.abs('num_terraza').cast('Integer').alias('nm_terrace_number'),
                          func.abs('num_depositos').cast('Integer').alias('nm_storage_room_number'),
                          func.abs('num_ascensores').cast('Integer').alias('nm_elevator_number'),
                          func.abs('num_parqueaderos_visitantes').cast('Integer').alias('nm_visitors_parking_number'),
                          func.col('recepcion').alias('fl_has_reception'),
                          func.col('sede_social').alias('fl_has_social_room'),
                          func.col('salon_comunal').alias('fl_has_communal_living'),
                          func.col('zona_infantil').alias('fl_has_children_zone'),
                          func.col('zonas_verdes').alias('fl_has_green_zones'),
                          func.col('piscina_comunal').alias('privatePool'),
                          func.col('gimnasio').alias('fl_has_gym'),
                          func.col('fecha_creacion').alias('dt_creation_date'),
                          func.col('fecha_modificacion').alias('dt_modification_date'),
                          func.col('permite_mascotas').cast('String').alias('allowPets'),
                          func.col('matricula_inmobiliaria').alias('ds_property_real_state_registration_number'),
                          func.col('enoferta').alias('fl_property_offer'),
                          func.col('start_publicacion').alias('dt_property_inception_date'),
                          func.col('end_publicacion').alias('dt_property_expiration_date'),
                          func.col('fecha_eliminacion').alias('dt_property_deletion_date'),
                          'nombre_proyecto',
                          'proyecto',
                          'vigilancia',
                          'activo')

inmueble = inmueble.join(estrato, inmueble.id_estrato == estrato.id_estrato, "left").drop('id_estrato')
inmueble = inmueble.join(ciudad, inmueble.id_ciudad == ciudad.id_ciudad, "left").drop('id_ciudad')
inmueble = inmueble.join(departamento, inmueble.id_departamento == departamento.id_departamento, "left").drop('id_departamento')
inmueble = inmueble.join(localidad, inmueble.id_localidad == localidad.id_localidad, "left").drop('id_localidad')
inmueble = inmueble.join(barrio, inmueble.id_barrio == barrio.id_barrio, "left").drop('id_barrio')
inmueble = inmueble.join(tipo_inmueble, inmueble.id_tipo_inmueble == tipo_inmueble.id_tipo_inmueble, "left").drop('id_tipo_inmueble')

inmueble = inmueble.fillna(value=0,subset=['area_bodega']).fillna(value=0,subset=['area_oficina']).fillna(value=0,subset=['area_lote']).fillna(value=0,subset=['area_construida']).fillna(value=0,subset=['nm_private_area'])

inmueble = inmueble.withColumn('nm_built_area', func.expr("CASE WHEN area_construida > 0 THEN area_construida " + 
                                                        "WHEN area_construida <= 0 and area_bodega > 0 THEN area_bodega " +
                                                        "WHEN area_construida <= 0 and area_lote > 0 THEN area_lote " +
                                                        "WHEN area_construida <= 0 and area_oficina > 0 THEN area_oficina " + 
                                                        "ELSE area_construida END"))

inmueble = inmueble.withColumn("ds_publication_status", func.expr("CASE WHEN activo = '0' THEN 'Activo' " + 
                                                        "WHEN activo = '1' THEN 'Inactivo' " +
                                                        "WHEN activo = '2' THEN 'Eliminado' " +
                                                        "WHEN activo = '4' THEN 'Repetido' " + 
                                                        "ELSE 'Otro' END")).drop('activo')

inmueble = inmueble.withColumn("ds_publication_site_store", func.expr("CASE WHEN id_tipo_transaccion = '1' THEN 'Venta' " + 
                                                        "WHEN id_tipo_transaccion = '2' THEN 'Arriendo' " +
                                                        "WHEN id_tipo_transaccion = '3' THEN 'Arriendo o venta' " + 
                                                        "WHEN id_tipo_transaccion = '4' THEN 'Agenda' " + 
                                                        "ELSE 'Otro' END")).drop('id_tipo_transaccion')
                                                        
inmueble = inmueble.withColumn("fl_has_vigilance", func.expr("CASE WHEN vigilancia in ('1','2') THEN 'Si' " + 
                                                        "ELSE 'No' END"))                                                       

inmueble = inmueble.withColumn('dt_property_project_delivery_date', func.lit(None))\
                    .withColumn('ds_publication_type', func.lit('Inmueble'))\
                    .withColumn('ds_property_project_building_class_name',func.lit(None))\
                    .withColumn('ds_property_project_building_class_status',func.lit(None))\
                    .withColumn('ds_property_project_name',func.lit(None))\
                    .withColumn('ds_property_project_status',func.lit(None))\
                    .withColumn('id_property_project',func.lit(None))\
                    .withColumn('id_property_project_building_class',func.lit(None))\
                    .withColumn('nm_square_meter_price',func.round(func.col('nm_property_selling_price') / func.col('nm_built_area'), 2))

inmueble = inmueble.join(usuario, inmueble.id_usuario == usuario.id_publication_owner_app_identification_number, "inner")
inmueble = inmueble.distinct()

inmueble = inmueble.withColumn("url",
            func.expr("CASE WHEN proyecto = 0 THEN concat('https://www.ciencuadras.com/inmueble/',lower(ds_property_type),'-en-',replace(trim(lower(ds_publication_site_store)),' ','-'),'-en-',replace(lower(ds_neighborhood),' ','-'),'-',replace(trim(lower(ds_city_name)),' ','-'),'-',id ) " +
                    "ELSE concat('https://www.ciencuadras.com/proyecto/proyecto-',replace(trim(lower(nombre_proyecto)),' ','-'),'-en-',replace(trim(lower(ds_neighborhood)),' ','-'),'-',replace(trim(lower(ds_city_name)),' ','-'),'-',id ) END"))
                    
inmueble = inmueble.withColumn("tx_url",func.expr("CASE WHEN url is not null THEN translate(url,'áéíóú','aeiou')" +
                                                    "ELSE 'https://www.ciencuadras.com/' END"))                    

inmuebles = inmueble.select('id_publication','ds_publication_status','ds_publication_type','ds_publication_site_store',
                            'id_publication_owner_app_identification_number','ds_publication_owner_email',
                            'ds_property_real_state_registration_number','ds_property_type','id_property_project',
                            'ds_property_project_name','ds_property_project_status','id_property_project_building_class',
                            'ds_property_project_building_class_name','ds_property_project_building_class_status',
                            'cd_state_dane_code','ds_state_name','cd_city_dane_code','ds_city_name','ds_neighborhood',
                            'ds_district_division','cd_neighborhood_economic_level','ds_address','ds_latitude','ds_longitude',
                            'nm_property_selling_price','nm_property_monthly_rent_payment','nm_property_monthly_administration_fee', 
                            func.col('nm_square_meter_price').cast('String').alias('nm_square_meter_price'),
                            func.col('nm_built_area').cast('String').alias('nm_built_area'),
                            func.col('nm_private_area').cast('String').alias('nm_private_area'),
                            func.col('nm_parking_number').cast('String').alias('nm_parking_number'),
                            func.col('nm_visitors_parking_number').cast('String').alias('nm_visitors_parking_number'),
                            func.col('nm_bedroom_number').cast('String').alias('nm_bedroom_number'),
                            func.col('nm_bathroom_number').cast('String').alias('nm_bathroom_number'),
                            func.col('nm_property_age').cast('String').alias('nm_property_age'),
                            func.col('nm_balcony_number').cast('String').alias('nm_balcony_number'),
                            func.col('nm_terrace_number').cast('String').alias('nm_terrace_number'),
                            func.col('nm_storage_room_number').cast('String').alias('nm_storage_room_number'),  
                            func.col('nm_elevator_number').cast('String').alias('nm_elevator_number'),
                            'allowPets','laundryZone','fl_has_green_zones','fl_has_communal_living','fl_has_children_zone',
                            'privatePool','fl_has_gym','serviceRoom','serviceBathroom','fl_has_social_room','fl_has_reception',
                            'airConditioner','homeAppliances','dt_creation_date','dt_modification_date',
                            'dt_property_inception_date','dt_property_expiration_date','dt_property_deletion_date',
                            'dt_property_project_delivery_date','fl_property_offer','tx_url','fl_has_vigilance')


tabla = inmuebles.union(proyectos)
tabla = tabla.na.drop(subset=["id_publication"])
tabla = tabla.distinct()

tabla = tabla.withColumn("fl_is_pet_allowed", func.expr("CASE WHEN allowPets = '0' THEN 'No' " + 
                                                        "WHEN allowPets = '1' THEN 'Si' " +
                                                        "ELSE 'No' END")).drop('allowPets')
                                                        
tabla = tabla.withColumn("fl_has_laundry_area", func.expr("CASE WHEN laundryZone = '0' or laundryZone = 'false' THEN 'No' " + 
                                                          "WHEN laundryZone = '1' or laundryZone = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('laundryZone')                                                        

tabla = tabla.withColumn("fl_has_green_zones", func.expr("CASE WHEN fl_has_green_zones = '0' or fl_has_green_zones = 'false' THEN 'No' " + 
                                                          "WHEN fl_has_green_zones = '1' or fl_has_green_zones = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END"))

tabla = tabla.withColumn("fl_has_communal_living", func.expr("CASE WHEN fl_has_communal_living = '0' or fl_has_communal_living = 'false' THEN 'No' " + 
                                                         "WHEN fl_has_communal_living = '1' or fl_has_communal_living = 'true' THEN 'Si' " +
                                                         "ELSE 'No' END"))                                                                

tabla = tabla.withColumn("fl_has_children_zone", func.expr("CASE WHEN fl_has_children_zone = '0' or fl_has_children_zone = 'false' THEN 'No' " + 
                                                          "WHEN fl_has_children_zone = '1' or fl_has_children_zone = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END"))

tabla = tabla.withColumn("fl_has_pool", func.expr("CASE WHEN privatePool = '0' or privatePool = 'false' THEN 'No' " + 
                                                          "WHEN privatePool = '1' or privatePool = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('privatePool')                                                  

tabla = tabla.withColumn("fl_has_gym", func.expr("CASE WHEN fl_has_gym = '0' or fl_has_gym = 'false' THEN 'No' " + 
                                                          "WHEN fl_has_gym = '1' or fl_has_gym = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END"))

tabla = tabla.withColumn("fl_has_service_room", func.expr("CASE WHEN serviceRoom = '0' or serviceRoom = 'false' THEN 'No' " + 
                                                          "WHEN serviceRoom = '1' or serviceRoom = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('serviceRoom')

tabla = tabla.withColumn("fl_has_service_bathroom", func.expr("CASE WHEN serviceBathroom = '0' or serviceBathroom = 'false' THEN 'No' " + 
                                                          "WHEN serviceBathroom = '1' or serviceBathroom = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('serviceBathroom')

tabla = tabla.withColumn("fl_has_reception", func.expr("CASE WHEN fl_has_reception = '0' or fl_has_reception = 'false' THEN 'No' " + 
                                                          "WHEN fl_has_reception = '1' or fl_has_reception = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END"))

tabla = tabla.withColumn("fl_has_air_conditioner", func.expr("CASE WHEN airConditioner = '0' or airConditioner = 'false' THEN 'No' " + 
                                                          "WHEN airConditioner = '1' or airConditioner = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('airConditioner')
                                              
tabla = tabla.withColumn("fl_is_property_offer", func.expr("CASE WHEN fl_property_offer = '0' or fl_property_offer = 'false' THEN 'No' " + 
                                                          "WHEN fl_property_offer = '1' or fl_property_offer = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END")).drop('fl_property_offer')

tabla = tabla.withColumn("fl_has_social_room", func.expr("CASE WHEN fl_has_social_room = '0' or fl_has_social_room = 'false' THEN 'No' " + 
                                                          "WHEN fl_has_social_room = '1' or fl_has_social_room = 'true' THEN 'Si' " +
                                                          "ELSE 'No' END"))

tabla = tabla.withColumn("fl_has_home_appliances", func.expr("CASE WHEN homeAppliances is null or homeAppliances = '0' THEN 'No' " + 
                                                          "WHEN homeAppliances = '1' THEN 'Si' " +
                                                          "ELSE 'Si' END")).drop('homeAppliances')

tabla = tabla.withColumn('dt_product_date_time', func.current_timestamp()).withColumn('dt_product_hudi_date_time', func.current_timestamp())

tabla = tabla.withColumn('dt_creation_date', func.to_timestamp('dt_creation_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_modification_date', func.to_timestamp('dt_modification_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_property_inception_date', func.to_timestamp('dt_property_inception_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_property_expiration_date', func.to_timestamp('dt_property_expiration_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_property_deletion_date', func.to_timestamp('dt_property_deletion_date', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_property_project_delivery_date', func.to_timestamp('dt_property_project_delivery_date', 'yyyy-MM-ddHH:mm:ss.SSSZ' ))

tabla = tabla.withColumn('dt_product_date_time', func.to_timestamp('dt_product_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('dt_product_hudi_date_time', func.to_timestamp('dt_product_hudi_date_time', 'yyyy-MM-ddHH:mm:ss.SSSZ'))
tabla = tabla.withColumn('nm_publication_days_since_posted', func.datediff(func.current_date(),func.col("dt_creation_date")))

table_pp = tabla.select('id_publication','ds_publication_status','ds_publication_type','ds_publication_site_store',
                      func.col('id_publication_owner_app_identification_number').cast('String').alias('id_publication_owner_app_identification_number'),
                      'ds_publication_owner_email','ds_property_real_state_registration_number','ds_property_type','id_property_project','ds_property_project_name',
                      'ds_property_project_status','id_property_project_building_class','ds_property_project_building_class_name','ds_property_project_building_class_status',
                      'fl_is_property_offer','cd_state_dane_code','ds_state_name','cd_city_dane_code','ds_city_name','ds_neighborhood','ds_district_division',
                      'cd_neighborhood_economic_level','ds_address','ds_latitude','ds_longitude',
                      func.col('nm_property_selling_price').cast('Decimal').alias('nm_property_selling_price'),
                      func.col('nm_property_monthly_rent_payment').cast('Decimal').alias('nm_property_monthly_rent_payment'),
                      func.col('nm_property_monthly_administration_fee').cast('Decimal').alias('nm_property_monthly_administration_fee'),
                      func.col('nm_square_meter_price').cast('Decimal').alias('nm_square_meter_price'),
                      func.col('nm_built_area').cast('Float').alias('nm_built_area'),
                      func.col('nm_private_area').cast('Float').alias('nm_private_area'),
                      func.col('nm_parking_number').cast('Integer').alias('nm_parking_number'),
                      func.col('nm_visitors_parking_number').cast('Integer').alias('nm_visitors_parking_number'),
                      func.col('nm_bedroom_number').cast('Integer').alias('nm_bedroom_number'),
                      func.col('nm_bathroom_number').cast('Integer').alias('nm_bathroom_number'),
                      func.col('nm_property_age').cast('Integer').alias('nm_property_age'),
                      func.col('nm_balcony_number').cast('Integer').alias('nm_balcony_number'),
                      func.col('nm_terrace_number').cast('Integer').alias('nm_terrace_number'),
                      'fl_is_pet_allowed','fl_has_laundry_area','fl_has_green_zones',
                      func.col('nm_elevator_number').cast('Integer').alias('nm_elevator_number'),
                      'fl_has_communal_living','fl_has_children_zone','fl_has_pool','fl_has_gym',
                      'fl_has_service_room','fl_has_service_bathroom','fl_has_social_room','fl_has_reception',
                      func.col('nm_storage_room_number').cast('Integer').alias('nm_storage_room_number'),
                      'fl_has_air_conditioner','fl_has_home_appliances','fl_has_vigilance',
                      func.col('nm_publication_days_since_posted').cast('Integer').alias('nm_publication_days_since_posted'),
                      'dt_property_project_delivery_date',
                      'tx_url',
                      'dt_creation_date',
                      'dt_modification_date',
                      'dt_property_inception_date',
                      'dt_property_expiration_date',
                      'dt_property_deletion_date',
                      'dt_product_date_time',
                      'dt_product_hudi_date_time')

table_pp = table_pp.fillna(value=0,subset=['nm_parking_number']).fillna(value=0,subset=['nm_visitors_parking_number'])\
    .fillna(value=0,subset=['nm_bedroom_number']).fillna(value=0,subset=['nm_bathroom_number']).fillna(value=0,subset=['nm_balcony_number'])\
    .fillna(value=0,subset=['nm_terrace_number']).fillna(value=0,subset=['nm_elevator_number']).fillna(value=0,subset=['nm_storage_room_number'])

table_pp = table_pp.distinct()
table_pp = table_pp.na.drop(subset=['id_publication','ds_publication_type','ds_publication_site_store','id_publication_owner_app_identification_number','ds_publication_owner_email','ds_state_name','ds_city_name'])

schema = func.StructType([
            StructField('id_publication', StringType(), False),
            StructField('ds_publication_status', StringType(), True),
            StructField('ds_publication_type', StringType(), False),
            StructField('ds_publication_site_store', StringType(), False),
            StructField('id_publication_owner_app_identification_number', StringType(), False),
            StructField('ds_publication_owner_email', StringType(), False),
            StructField('ds_property_real_state_registration_number', StringType(), True),
            StructField('ds_property_type', StringType(), True),  
            StructField('id_property_project', StringType(), True),
            StructField('ds_property_project_name', StringType(), True),
            StructField('ds_property_project_status', StringType(), True),
            StructField('id_property_project_building_class', StringType(), True),
            StructField('ds_property_project_building_class_name', StringType(), True),
            StructField('ds_property_project_building_class_status', StringType(), True),
            StructField('fl_is_property_offer', StringType(), True),
            StructField('cd_state_dane_code', StringType(), False),
            StructField('ds_state_name', StringType(), False),
            StructField('cd_city_dane_code', StringType(), False),
            StructField('ds_city_name', StringType(), False),
            StructField('ds_neighborhood', StringType(), True),
            StructField('ds_district_division', StringType(), True),
            StructField('cd_neighborhood_economic_level', StringType(), True),
            StructField('ds_address', StringType(), True),
            StructField('ds_latitude', StringType(), True),
            StructField('ds_longitude', StringType(), True),
            StructField('nm_property_selling_price', DecimalType(18,2), False),
            StructField('nm_property_monthly_rent_payment', DecimalType(18,2), False),
            StructField('nm_property_monthly_administration_fee', DecimalType(18,2), False),
            StructField('nm_square_meter_price', DecimalType(18,2), False),
            StructField('nm_built_area', FloatType(), False),
            StructField('nm_private_area', FloatType(), False),                        
            StructField('nm_parking_number', IntegerType(), True),
            StructField('nm_visitors_parking_number', IntegerType(), True),
            StructField('nm_bedroom_number', IntegerType(), True),
            StructField('nm_bathroom_number', IntegerType(), True),
            StructField('nm_property_age', IntegerType(), True),
            StructField('nm_balcony_number', IntegerType(), True),
            StructField('nm_terrace_number', IntegerType(), True), 
            StructField('fl_is_pet_allowed', StringType(), True),
            StructField('fl_has_laundry_area', StringType(), True),
            StructField('fl_has_green_zones', StringType(), True),
            StructField('nm_elevator_number', IntegerType(), True),
            StructField('fl_has_communal_living', StringType(), True),
            StructField('fl_has_children_zone', StringType(), True),
            StructField('fl_has_pool', StringType(), True),
            StructField('fl_has_gym', StringType(), True),
            StructField('fl_has_service_room', StringType(), True),
            StructField('fl_has_service_bathroom', StringType(), True),
            StructField('fl_has_social_room', StringType(), True),
            StructField('fl_has_reception', StringType(), True),
            StructField('nm_storage_room_number', IntegerType(), True), 
            StructField('fl_has_air_conditioner', StringType(), True),
            StructField('fl_has_home_appliances', StringType(), True), 
            StructField('fl_has_vigilance', StringType(), True),
            StructField('nm_publication_days_since_posted', IntegerType(), True), 
            StructField('dt_property_project_delivery_date', TimestampType(), True),
            StructField('tx_url', StringType(), True),
            StructField('dt_creation_date', TimestampType(), False),
            StructField('dt_modification_date', TimestampType(), True),
            StructField('dt_property_inception_date', TimestampType(), True),
            StructField('dt_property_expiration_date', TimestampType(), True),
            StructField('dt_property_deletion_date', TimestampType(), True),
            StructField('dt_product_date_time', TimestampType(), True),
            StructField('dt_product_hudi_date_time', TimestampType(), True)
            ])

emptyRDD = spark.sparkContext.emptyRDD()
properties_projects = spark.createDataFrame(emptyRDD, schema)            
properties_projects = properties_projects.union(table_pp)

ciencuadras_products_properties_and_projects_glue_tb = DynamicFrame.fromDF(properties_projects, glueContext, "ciencuadras_products_properties_and_projects_glue_tb")

gl2.upsert_hudi_table(
    spark_dyf = ciencuadras_products_properties_and_projects_glue_tb,
    glue_database = f"{dbName}",
    table_name = "ciencuadras_products_properties_and_projects_glue_tb",
    record_id = 'id_publication',
    precomb_key = 'dt_product_hudi_date_time',
    overwrite_precomb_key = True,
    target_path = f"s3://{targetBucketName}/{route}/{dbName}/{prefixTable}properties_and_projects{suffixTable}/",
)