
# --------------------------------------
# IAM ROLE GLUE
# --------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "${var.proyecto}-${var.dominio}-${var.ambiente}-accesoglue-iam-rol"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    tag-key = "glue-role-ciencuadras"
  }
}

resource "aws_iam_policy" "AwsGLUES3RoleTerra" {
  name   = "${var.proyecto}-${var.dominio}-${var.ambiente}-accesobuckets-iam-pl"
  policy = data.template_file.json_template_policy_glue.rendered
}

resource "aws_iam_role_policy_attachment" "glue-access-for-resources-GLUES3DataOperativaRole" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.AwsGLUES3RoleTerra.arn
}

resource "aws_glue_job" "glue_job" {
  for_each          = local.processing_glue_jobs
  name              = "${var.ambiente}-${each.value["capaalmacenamientoorigen"]}-${each.value["capaalmacenamientodestino"]}-${each.value["propositoprocesamiento"]}-${each.value["fuente"]}-glue-job"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = each.value["glue_version"]
  worker_type       = "G.1X"
  number_of_workers = each.value["number_of_workers"]
  max_retries       = 0

  default_arguments = merge({
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    # "--enable-spark-ui"                  = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--datalake-formats"                 = "hudi"
    "--conf"                             = "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false"
    "--TempDir"                          = "s3://${module.buckets_s3["dependencies"].name}/glueAssets/sparkHistoryLogs/"
    # "--spark-event-logs-path"            = "s3://${module.buckets_s3["dependencies"].name}/glueAssets/temporary/"
    "--extra-py-files"                   = "${join(",", [for library in each.value["libraries"] : "s3://${module.buckets_s3["dependencies"].name}/${library}"])}"

    "--BUCKET_CONF"       = module.buckets_s3["dependencies"].name
    "--BUCKET_ORIG"       = module.buckets_s3["${each.value.capaalmacenamientoorigen}"].name
    "--BUCKET_DEST"       = module.buckets_s3["${each.value.capaalmacenamientodestino}"].name
    "--FORMAT"            = each.value.format
    "--DB_NAME"           = "rl_${var.dominio}_glue_db"
    "--PREFIX_TABLE_DEST" = "${var.dominio}_${each.value.capaalmacenamientodestino}_"
    "--SUFIX_TABLE_DEST"  = "_glue_tb"
    "--ROUTE"             = "ciencuadras"
    # "--OVERWRITE_TABLE" = ""
  }, each.value.extra_parameters)

  command {
    script_location = "s3://${module.buckets_s3["dependencies"].name}/${each.value["file_name"]}"
  }
}