
# -------------------------------------------
# DATABASE MIGRATION SERVICE ECOSISTEMA HOME-
# -------------------------------------------

module "database_migration_service" {
  source = "git::https://github.com/segurosbolivar/libreria-modulos-comunes-infra.git//modules/migration-transfer/dms?ref=v2.6.1"

  role_suffix                   = var.dominio
  stack_id                      = var.ambiente
  repl_subnet_group_name        = "${var.proyecto}-${var.dominio}-${var.ambiente}-replication-subnet-group"
  repl_subnet_group_description = "DMS Subnet group for ecosistema home"
  repl_subnet_group_subnet_ids  = module.network.private_subnet_ids

  repl_instance_id                           = "${var.proyecto}-${var.dominio}-${var.ambiente}-dms-ri"
  repl_instance_class                        = "dms.t3.large"
  repl_instance_allocated_storage            = 64
  repl_instance_multi_az                     = false
  repl_instance_vpc_security_group_ids       = [aws_security_group.dms_sg.id]
  repl_instance_engine_version               = "3.4.7"
  repl_instance_auto_minor_version_upgrade   = true
  repl_instance_allow_major_version_upgrade  = true
  repl_instance_apply_immediately            = true
  repl_instance_publicly_accessible          = false
  repl_instance_preferred_maintenance_window = "sun:10:30-sun:14:30"


  endpoints = {

    source1 = {
      endpoint_id                 = "${var.ambiente}-mysql-ciencuadras-source-dms-ep"
      endpoint_type               = "source"
      engine_name                 = "mysql"
      server_name                 = var.dbhost_mysql
      username                    = data.aws_ssm_parameter.user.value
      password                    = data.aws_ssm_parameter.pass.value
      port                        = var.dbport_mysql
      database_name               = var.dbname_mysql
      extra_connection_attributes = ""
      ssl_mode                    = "none"
      tags                        = { EndpointType = "source" }
    }


    target1 = {
      endpoint_id   = "${var.ambiente}-s3-ciencuadras-target-dms-ep"
      endpoint_type = "target"
      engine_name   = "s3"

      s3_settings = {
        bucket_name             = module.buckets_s3["raw"].name
        bucket_folder           = "ciencuadras"
        data_format             = "parquet"
        date_partition_enabled  = false
        compression_type        = "NONE"
        service_access_role_arn = aws_iam_role.dms_role.arn
        timestamp_column_name   = "field_timestamp"
      }
    }

  }

  replication_tasks = {

    mysql_fl_cdc = {
      replication_task_id       = "${var.ambiente}-mysql-ciencuadras-s3-target-dms-rt"
      migration_type            = "full-load-and-cdc"
      replication_task_settings = file("./dms-configs/mysql/settings.json")
      table_mappings            = file("./dms-configs/mysql/mappings.json")
      source_endpoint_key       = "source1"
      target_endpoint_key       = "target1"
      tags                      = { Task = "Mysql-to-S3" }
    }
  }

  tags = merge(var.tags, { Name = "DMS", Terraform = "true" })
}


# --------------------------------------
# IAM
# --------------------------------------
resource "aws_iam_role" "dms_role" {
  name = "${var.proyecto}-${var.dominio}-${var.ambiente}-dms-s3access-iam-rol"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "dms.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    tag-key = "dms-ecosistema-home"
  }
}

#-------------------------------------#
# Politica para acceso a s3 desde dms #
#-------------------------------------#
resource "aws_iam_policy" "AwsDmsS3Role" {
  name   = "${var.proyecto}-${var.dominio}-${var.ambiente}-dmsaccess-iam-pl"
  policy = data.template_file.json_template_policy_dms.rendered
}

resource "aws_iam_role_policy_attachment" "dms-access-for-endpoint-AmazonDmsS3Role" {
  role       = aws_iam_role.dms_role.name
  policy_arn = aws_iam_policy.AwsDmsS3Role.arn
}


# Peering para Mysql

# resource "aws_route" "subnet_mysql" {
#   count                     = length(var.route_table_vpc)
#   route_table_id            = var.route_table_vpc[count.index]
#   destination_cidr_block    = "10.50.0.0/16"
#   vpc_peering_connection_id = var.peering //ID PEERING
# }

# dms database credentials ssm parameter

data "aws_ssm_parameter" "user" {
  name            = var.ssm_dbuser_mysql
  with_decryption = true
}
data "aws_ssm_parameter" "pass" {
  name            = var.ssm_dbpass_mysql
  with_decryption = true
}