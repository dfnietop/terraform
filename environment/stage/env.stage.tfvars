# General
region         = "us-east-1"
proyecto       = "do"
dominio        = "ciencuadras"
ambiente       = "stg"
tags           = { "ambiente" = "stg", "dominio" = "ciencuadras", "environment" = "stg" }
type           = "infra"

# VPC
component_name     = "ecosistema-home"
azs                = ["us-east-1a", "us-east-1b", "us-east-1c"] # AZs are region specific
vpc_cidr           = "10.67.157.0/25"
private_subnets    = ["10.67.157.0/27",	"10.67.157.32/27","10.67.157.64/28"]    # Creating one private subnet per AZ
public_subnets     = ["10.67.157.80/28",	"10.67.157.96/28"	,"10.67.157.112/28"] # Creating one public subnet per AZ
transit_gateway_id = "tgw-07ae85afb4ba519d9"
peering= "pcx-00d2381d8b0c56599"

# MySql
dbhost_mysql = "qa-rds-ciencuadras-auroracluster-1ehdykxhsycc72-cluster-prod.cluster-cmge2sgqpfjt.us-east-1.rds.amazonaws.com"
ssm_dbpass_mysql = "/do/ciencuadras/stg/c1cc_new/db_password"
ssm_dbuser_mysql = "/do/ciencuadras/stg/c1cc_new/db_username"
dbport_mysql =  3306
dbname_mysql = "c1cc_new"

# Lake Formation

administrato_user_arn = "arn:aws:iam::505545527670:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_7d2ea6e0cdae127b"
owner_ciencuadras         = "505545527670"
owner_gobierno        = "762943499080"
area                  = "dominio"
value_ciencuadras         = "ciencuadras"

viewer_users_arn = [
  "arn:aws:iam::505545527670:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_ViewOnlyAccess_029ee60c0132aaf8",
  "arn:aws:iam::505545527670:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_ejecucion_glue_y_athena_10d28f8cd1792c9b",
  "arn:aws:quicksight:us-east-1:505545527670:group/default/visorlineanegocio"
]

all_databases = [
  {
    name          = "rl_ciencuadras_glue_db"
    database_name = "ciencuadras-glue-db"
    catalog_id    = "762943499080"
  },
  {
    name          = "default"
    database_name = ""
    catalog_id    = ""
  }
]

sfn_statemachines = {
  ciencuadras = {
    definition_path = "./step-definition/fl-cdc-ciencuadras.json"
    schedule_expression = "cron(0 12 ? * MON-FRI *)"
    start_date = "2023-08-09T01:00:00Z"
    end_date = "2023-08-10T01:00:00Z"
  }
}

monitoring_emails = ["juan.amaya@bluetab.net","angelica.pinzon@bluetab.net","rpalacio@qvision.us"]