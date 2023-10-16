# General
region         = "us-east-1"
proyecto       = "do"
dominio        = "ciencuadras"
ambiente       = "dev"
tags           = { "ambiente" = "dev", "dominio" = "ciencuadras", "environment" = "dev" }
type           = "infra"

# VPC
component_name     = "ecosistema-home"
azs                = ["us-east-1a", "us-east-1b", "us-east-1c"] # AZs are region specific
vpc_cidr           = "10.67.156.128/25"
private_subnets    = ["10.67.156.128/27",	"10.67.156.160/27","10.67.156.192/28"]    # Creating one private subnet per AZ
public_subnets     = ["10.67.156.208/28",	"10.67.156.224/28"	,"10.67.156.240/28"] # Creating one public subnet per AZ
transit_gateway_id = "tgw-07ae85afb4ba519d9"
peering= "pcx-0f2fa1a40268a9f96"

# MySql
dbhost_mysql = "dev-rds-ciencuadras-prod-cluster.cluster-cmge2sgqpfjt.us-east-1.rds.amazonaws.com"
ssm_dbpass_mysql = "/do/ciencuadras/dev/c1cc_new/db_password"
ssm_dbuser_mysql = "/do/ciencuadras/dev/c1cc_new/db_username"
dbport_mysql =  3306
dbname_mysql = "c1cc_new"

# Lake Formation

administrato_user_arn = "arn:aws:iam::565999949569:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_a45db5ae61c05b5c"
owner_ciencuadras         = "565999949569"
owner_gobierno        = "883623702261"
area                  = "dominio"
value_ciencuadras         = "ciencuadras"

viewer_users_arn = [
  "arn:aws:iam::565999949569:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_ViewOnlyAccess_3e84dae55379c7cd",
  "arn:aws:quicksight:us-east-1:565999949569:group/default/visorlineanegocio"
]


all_databases = [
  {
    name          = "rl_ciencuadras_glue_db"
    database_name = "ciencuadras-glue-db"
    catalog_id    = "883623702261"
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
    schedule_expression = "cron(0 12-23 ? * MON-FRI *)"
    start_date = null
    end_date = null
  }
}

monitoring_emails = ["juan.amaya@bluetab.net","angelica.pinzon@bluetab.net"]