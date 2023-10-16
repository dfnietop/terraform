# LIBRERIA PARA  FACILITAR LA EJECUCION EN GLUE DE LA TRANSFORMACION DE DATOS
import boto3, json, datetime, pytz
from typing import Optional, Callable
from awsglue.context import GlueContext, DynamicFrame
from awsglue.transforms import ApplyMapping
from pyspark.sql.functions import monotonically_increasing_id, from_json, col, explode_outer, lit, concat
from pyspark.sql.types import StructType, ArrayType

# 1. metodo que permita leer de cualquier tipo de fuente desde S3 en formato csv, json, parquet) -> dataframe (glue)
# 2. metodo escritura hudi a s3
# 3. metodo que pipe para transformaciones
# 4. metodo para manejo de llaves primarias -> opcional recompose key
# 5. metodo de renombrado de columnas

class NotFormatFoundError(Exception):
    pass


def get_date_time():
    tz = pytz.timezone('America/Bogota')
    dt = datetime.datetime.now(tz=tz)
    fecha = dt.strftime('%Y-%m-%d')
    hora = dt.strftime('%H:%M:%S')
    return concat(lit(fecha), lit(' '), lit(hora))


def rename_field(df, value, change):
  for name in df.schema.names:
    df = df.withColumnRenamed(name, name.replace(value, change))
  return df


def complete_columns(df, df_all_cols):
    for column in [column for column in df_all_cols.dtypes if column[0] not in df.columns]:
        df_complete = df.withColumn(column[0], lit(None).cast(column[1]))
    return df_complete


def execFlatten(input_df, primary_partition_column, json_column_name, spark_session):
    json_df = get_json_df(input_df, primary_partition_column, json_column_name, spark_session)
    unstd_df = execute_autoflatten(json_df, json_column_name)
    return unstd_df


def get_json_df(input_df, primary_partition_column, json_column_name, spark_session):

    input_df = input_df if primary_partition_column is None else input_df.drop(primary_partition_column)
    # creating a column transformedJSON to create an outer struct
    df1 = input_df.withColumn('transformed_json', concat(lit("""{"transformedJSON" :"""), input_df[json_column_name], lit("""}""")))
    json_df = spark_session.read.json(df1.rdd.map(lambda row: row.transformed_json))
    # get schema
    json_schema = json_df.schema
    df = df1.drop(json_column_name).withColumn(json_column_name, from_json(col('transformed_json'), json_schema)).drop('transformed_json').select(f'{json_column_name}.*', '*').drop(json_column_name)
    return df


def execute_autoflatten(df, json_column_name):

    # gets all fields of StructType or ArrayType in the nested_fields dictionary
    nested_fields = dict([
        (field.name, field.dataType)
        for field in df.schema.fields
        if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
    ])

    # repeat until all nested_fields i.e. belonging to StructType or ArrayType are covered
    while nested_fields:
        # if there are any elements in the nested_fields dictionary
        if nested_fields:
            # get a column
            column_name = list(nested_fields.keys())[0]
            # if field belongs to a StructType, all child fields inside it are accessed
            # and are aliased with complete path to every child field
            if isinstance(nested_fields[column_name], StructType):
                unnested = [col(column_name + '.' + child).alias(column_name + '_' + child) for child in [ n.name for n in  nested_fields[column_name]]]
                df = df.select("*", *unnested).drop(column_name)
            # else, if the field belongs to an ArrayType, an explode_outer is done
            elif isinstance(nested_fields[column_name], ArrayType):
                df = df.withColumn(column_name, explode_outer(column_name))

        # Now that df is updated, gets all fields of StructType and ArrayType in a fresh nested_fields dictionary
        nested_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType)
        ])

    # renaming all fields extracted with json> to retain complete path to the field
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace("transformedJSON", json_column_name))
    df = df.select([col(c).alias(c.replace("_doc_", "")) for c in df.columns])
    return df


def read_data(glueContext:GlueContext, s3_url: str, data_type: str) -> DynamicFrame:
    """
        MÃ©todo para leer datos de un bucket S3 y retornar un awsglue.DynamicFrame

        Args:
            glueContext: Contexto glue del job.
            s3_url: Url del bucket de s3 con los datos.
            data_type: formato de archivo en el que se encuentran los datos (parquet, json, csv). 

        Returns:
            awsglue.DynamicFrame con los datos leidos de la ruta del bucket.

        Raises:
            NotFormatFoundError: Raises an exception.
    """
    readed_df = None
    path = s3_url
    if data_type == 'parquet':
        readed_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="parquet"
        )
    elif data_type == 'csv':
        readed_df = glueContext.create_dynamic_frame.from_options(
            format_options={
                "quoteChar": '"',
                "withHeader": True,
                "separator": ",",
                "optimizePerformance": False,
            },
            connection_type="s3",
            format="csv",
            connection_options={
                            "paths": [path],
                            "recurse": True,
            }
        )
    elif data_type == 'json':
        readed_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="json",
            format_options={
                            "jsonPath": "$.id",
                            "multiline": True,
                            # "optimizePerformance": True, -> not compatible with jsonPath, multiline
            }
        )
    else:
        raise NotFormatFoundError()
    return readed_df


def write_to_hudi_format(glueContext:GlueContext, s3_destiny_url: str, db_name: str, table: str, primary_key_field: str, resultDf: DynamicFrame):
    """
        MÃ©todo para realizar escritura en formato hudi en s3 de stage

        Args:
            glueContext: Contexto glue del job.
            s3_destiny_url: Url del bucket de s3 donde escribir.
            db_name: Nombre de la base de datos.
            table: Nombre de la tabla. 
            primary_key_field: Campo de llave primaria de la tabla.
            resultDf: DynamicFrame con los datos a ser escritos en formato hudi.
    """
    commonConfig = {'className': 'org.apache.hudi', 'hoodie.datasource.hive_sync.use_jdbc': 'false',
                    'hoodie.datasource.write.recordkey.field': primary_key_field,
                    'hoodie.table.name': "{t}".format(t=table),
                    'hoodie.consistency.check.enabled': 'true',
                    'hoodie.datasource.hive_sync.database': db_name,
                    'hoodie.datasource.hive_sync.table': "{t}".format(t=table),
                    'hoodie.datasource.hive_sync.enable': 'true',
                    'hoodie.parquet.writelegacyformat.enabled': 'false',
                    'hoodie.parquet.field_id.write.enabled': 'true',
                    'path': "s3://{d}/{k}".format(d=s3_destiny_url, k=table)}
    unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
                             'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
    initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 8,
                      'hoodie.datasource.write.operation': 'insert_overwrite'}
    combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
    glueContext.write_dynamic_frame.from_options(frame=resultDf, connection_type="marketplace.spark",
                                                 connection_options=combinedConf)

def write_to_hudi_format_append(glueContext:GlueContext, s3_destiny_url: str, db_name: str, table: str, primary_key_field: str, resultDf: DynamicFrame):
    """
        MÃ©todo para realizar escritura en formato hudi en s3 de stage

        Args:
            glueContext: Contexto glue del job.
            s3_destiny_url: Url del bucket de s3 donde escribir.
            db_name: Nombre de la base de datos.
            table: Nombre de la tabla. 
            primary_key_field: Campo de llave primaria de la tabla.
            resultDf: DynamicFrame con los datos a ser escritos en formato hudi.
    """
    commonConfig = {'className': 'org.apache.hudi', 
                    'hoodie.datasource.hive_sync.use_jdbc': 'false',
                    'hoodie.datasource.write.recordkey.field': primary_key_field,
                    'hoodie.table.name': "{t}".format(t=table),
                    'hoodie.consistency.check.enabled': 'true',
                    'hoodie.datasource.hive_sync.database': db_name,
                    'hoodie.datasource.hive_sync.table': "{t}".format(t=table),
                    'hoodie.datasource.hive_sync.enable': 'true',
                    'hoodie.parquet.writelegacyformat.enabled': 'false',
                    'hoodie.parquet.field_id.write.enabled': 'true',
                    'path': "s3://{d}/{k}".format(d=s3_destiny_url, k=table)}
    unpartitionDataConfig = {'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
                             'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}
    initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 8,
                      'hoodie.datasource.write.operation': 'insert'}
    combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
    glueContext.write_dynamic_frame.from_options(frame=resultDf, connection_type="marketplace.spark",
                                                 connection_options=combinedConf)

def create_primary_keys(glueContext:GlueContext,target_df: DynamicFrame, field_pk: str) -> DynamicFrame:
    """
        MÃ©todo para mapear campos para primary keys

        Args:
            glueContext: Contexto glue del job.
            target_df: DynamicFrame donde se va a crear el nuevo campo.
            field_pk: Nombre del nuevo campo para la llave primaria. 

        Returns:
            awsglue.DynamicFrame con la llave primaria creada.
    """
    spark_df = target_df.toDF()
    spark_df = spark_df.withColumn(field_pk, monotonically_increasing_id() + 1)
    return DynamicFrame.fromDF(spark_df, glueContext, "result_df")


def mapping_fields(target_df: DynamicFrame, bucket_name: str, mapping_file: str ) -> DynamicFrame:
    """
        MÃ©todo para renombrar columnas y hacer cambios de tipo de datos

        Args:
            target_df: DynamicFrame donde se va a realizar el mapeo de los datos.
            bucket_name: Nombre del bucket donde esta archivo de mapeo.
            mapping_file:  Ruta del archivo de mapeo.

        Returns:
            awsglue.DynamicFrame con los campos mapeados de acuerdo a la estructura json.
        
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=mapping_file)
    mappings = json.loads(response['Body'].read())['columnas']
    return mapping_fields_with_dict(target_df, mappings)


def mapping_fields_with_dict(target_df: DynamicFrame, mappings: dict ) -> DynamicFrame:
    """
        MÃ©todo para renombrar columnas y hacer cambios de tipo de datos

        Args:
            target_df: DynamicFrame donde se va a realizar el mapeo de los datos.
            mappings: dict con la estructura de mapeo.

        Returns:
            awsglue.DynamicFrame con los campos mapeados de acuerdo a la estructura json.
        
    """
    columnas = []
    for value in mappings.values():
        mapping = (value['old']['name'], value['old']['type'],
                   value['new']['name'], value['new']['type'])
        columnas.append(mapping)
    result_df = ApplyMapping.apply(
        frame=target_df,
        mappings=columnas
    )
    return result_df


def load_mapping_config(bucket_name : str, mapping_file: str) -> dict:
    """
        MÃ©todo que descarga las configuraciones de estructura de una tabla subida en s3
    """
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=mapping_file)
    return json.loads(response['Body'].read())


def execute_process(glueContext:GlueContext,s3_origin_url: str, s3_target_url: str, data_type: str, db_name: str, table: str, 
                    primary_key_field: str, create_pk: bool = False, mapping_file_url: Optional[str] = None,
                    bucket_name: Optional[str] = None, 
                    transformation_function: Optional[Callable[[DynamicFrame], DynamicFrame]] = None):
    """
        MÃ©todo para ejecutar procesamiento de datos

        Args:
            glueContext: Contexto glue del job.
            s3_origin_url: Ruta de origen de los datos en s3.
            s3_target_url:  Ruta de destino de los datos en s3.
            data_type: formato de archivo en el que se encuentran los datos (parquet, json, csv).
            db_name: Nombre de la base de datos.
            table: Nombre de la tabla.
            primary_key_field: Nombre del campo de llave primaria.
            create_pk: True se crea el campo de llave primaria, False el campo ya existe en los datos.
            mapping_file_url: Ruta de archivo de estructura de mapeo de tabla.[OPCIONAL] 
            bucket_name: Nombre del bucket donde se encuentra el archivo de mapeo.[OPCIONAL]
            transformation_function: FunciÃ³n con transformaciones adicionales sobre los datos. [OPCIONAL]

    """
    origin_df = read_data(glueContext, s3_origin_url, data_type)
    if mapping_file_url is not None:
        origin_df = mapping_fields(origin_df, bucket_name, mapping_file_url)
    if create_pk:
        origin_df = create_primary_keys(glueContext, origin_df, primary_key_field)
    if transformation_function is not None:
        origin_df = transformation_function(origin_df)
    write_to_hudi_format(glueContext, s3_target_url, db_name, table, primary_key_field, origin_df)

  
def execute_process_with_config_file(glueContext:GlueContext,
                                     s3_origin_url: str, 
                                     s3_target_url: str, 
                                     data_type: str, 
                                     db_name: str, 
                                     table: str, 
                                     create_pk: bool = False, 
                                     config_file_url: Optional[str] = None,
                                     bucket_name: Optional[str] = None, 
                                     transformation_function: Optional[Callable[[DynamicFrame], DynamicFrame]] = None):
    """
        MÃ©todo para ejecutar procesamiento de datos

        Args:
            glueContext: Contexto glue del job.
            s3_origin_url: Ruta de origen de los datos en s3.
            s3_target_url:  Ruta de destino de los datos en s3.
            data_type: formato de archivo en el que se encuentran los datos (parquet, json, csv).
            db_name: Nombre de la base de datos.
            table: Nombre de la tabla.
            create_pk: True se crea el campo de llave primaria, False el campo ya existe en los datos.
            config_file_url: Ruta de archivo de estructura de mapeo de tabla.[OPCIONAL] 
            bucket_name: Nombre del bucket donde se encuentra el archivo de mapeo.[OPCIONAL]
            transformation_function: FunciÃ³n con transformaciones adicionales sobre los datos. [OPCIONAL]
    """
    config = load_mapping_config(bucket_name, config_file_url)
    origin_df = read_data(glueContext, s3_origin_url, data_type)
    if 'columnas' in config:
        origin_df = mapping_fields_with_dict(origin_df, config['columnas'])
    if create_pk:
        origin_df = create_primary_keys(glueContext, origin_df, config['primary_key'])
    if transformation_function is not None:
        origin_df = transformation_function(origin_df)
    write_to_hudi_format(glueContext, s3_target_url, db_name, table, config['primary_key'], origin_df)