# General
region         = "us-east-1"
proyecto       = "do"
dominio        = "ciencuadras"
ambiente       = "prod"
tags           = { "ambiente" = "prod", "dominio" = "ciencuadras", "environment" = "prod" }
type           = "infra"

# VPC
component_name     = "ecosistema-home"
azs                = ["us-east-1a", "us-east-1b", "us-east-1c"] # AZs are region specific
vpc_cidr           = "10.67.216.0/24"
private_subnets    = ["10.67.216.0/26", "10.67.216.64/26", "10.67.216.128/26"]    # Creating one private subnet per AZ
public_subnets     = ["10.67.216.192/28", "10.67.216.208/28", "10.67.216.224/28"] # Creating one public subnet per AZ
transit_gateway_id = "tgw-07ae85afb4ba519d9"
peering= "pcx-00e479ccb210967cb"

# MySql
dbhost_mysql = "www-rds-production-cluster.cluster-cmge2sgqpfjt.us-east-1.rds.amazonaws.com"
ssm_dbpass_mysql = "/do/ciencuadras/prod/c1cc_new/db_password"
ssm_dbuser_mysql = "/do/ciencuadras/prod/c1cc_new/db_username"
dbport_mysql =  3306
dbname_mysql = "c1cc_new"

# Lake Formation

administrato_user_arn = "arn:aws:iam::290296201161:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_3c29eccf6de3e680"
owner_ciencuadras         = "290296201161"
owner_gobierno        = "385571785903"
area                  = "dominio"
value_ciencuadras         = "ciencuadras"



viewer_users_arn = ["arn:aws:quicksight:us-east-1:290296201161:group/default/visorlineanegocio"]

all_databases = [
  {
    name          = "rl_ciencuadras_glue_db"
    database_name = "ciencuadras-glue-db"
    catalog_id    = "385571785903"
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
    schedule_expression = "cron(0 * ? * * *)"
    start_date = null
    end_date = null
  }
}
monitoring_emails = ["juan.amaya@bluetab.net","angelica.pinzon@bluetab.net","sandra.cuesta@segurosbolivar.com","ricardo.florez@segurosbolivar.com"]