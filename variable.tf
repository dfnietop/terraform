variable "ambiente" {
  description = "Nombre del ambiente"
  type        = string
}

variable "dominio" {
  description = "dominio"
  type        = string
}

variable "proyecto" {
  description = "proyecto"
  type        = string
}


variable "type" {
  description = "Tipo del recurso, infra, frontend, movil, backend"
  type        = string
}

variable "component_name" {
  description = "Nombre del componente"
  type        = string
}

variable "region" {
  default = "us-east-1"
}
variable "tags" {
  description = "Nombre del proyecto"
  type        = map(any)
}


variable "vpc_cidr" {
  description = "Vpc cidr block"
  type        = string
}

variable "azs" {
  description = "Availability Zones"
  type        = list(string)
}

variable "public_subnets" {
  description = "subnets public vpc"
  type        = list(string)
}

variable "private_subnets" {
  description = "subnets private vpc"
  type        = list(string)
}

variable "transit_gateway_id" {
  description = "transit_gateway_id"
  type        = string
}

# Credenciales MySql

variable "dbhost_mysql" {
  description = "dbhost_mysql"
  type        = string
}
variable "dbport_mysql" {
  description = "dbport_mysql"
  type        = string
  default     = 5432
}
variable "dbname_mysql" {
  description = "dbname_mysql"
  type        = string
}

variable "ssm_dbuser_mysql" {
  description = "ssm_dbuser_mysql"
  type        = string
}
variable "ssm_dbpass_mysql" {
  description = "ssm_dbpass_mysql"
  type        = string
}

# Peering MySql

# variable "route_table_vpc" {
#   description = "Tablas de enrutamiento"
#   type        = list(string)
# }

variable "peering" {
  description = "vpc peering id"
  type        = string
}

# Lakeformation

variable "administrato_user_arn" {
  description = "arn del usuario administrador"
  type        = string
}

variable "viewer_users_arn" {
  type        = set(string)
  description = "arn de el/los usuarios con permiso de lectura"
}

variable "all_databases" {
  description = "all_resource_links"
  type        = list(object({ name = string, database_name = string, catalog_id = string }))
}

variable "owner_ciencuadras" {
  description = "owner_ciencuadras"
  type        = string
}

variable "owner_gobierno" {
  description = "owner_gobierno"
  type        = string
}

variable "area" {
  description = "area"
  type        = string
}

variable "value_ciencuadras" {
  description = "value_ciencuadras"
  type        = string
}

variable "sfn_statemachines" {
  description = "sfn state machines"
  type = map(object({
    definition_path     = string
    schedule_expression = string
    start_date          = string
    end_date            = string
  }))
}

variable "monitoring_emails" {
  type        = set(string)
  description = "arn de el/los usuarios con permiso de lectura"
}