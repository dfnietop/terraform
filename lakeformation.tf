# Recursos para LakeFormation 

# Configuración para que lake formation tenga accesos suficientes 
resource "aws_lakeformation_data_lake_settings" "access" {

  admins = [
    data.aws_iam_session_context.current.arn,
    var.administrato_user_arn
  ]
}

# Creación resource link y bd default
resource "aws_glue_catalog_database" "aws_glue_catalog_db" {
  count = length(var.all_databases)
  name  = var.all_databases[count.index].name
  depends_on = [
    aws_lakeformation_data_lake_settings.access
  ]
  target_database {
    catalog_id    = var.all_databases[count.index].catalog_id
    database_name = var.all_databases[count.index].database_name
  }
}

# Data locations

resource "aws_lakeformation_resource" "bucket-curated" {
  arn = module.buckets_s3["curated"].arn
}

resource "aws_lakeformation_resource" "bucket-products" {
  arn = module.buckets_s3["products"].arn
}

resource "aws_lakeformation_permissions" "data_location_access" {

  depends_on = [
    aws_lakeformation_resource.bucket-curated
  ]
  principal   = aws_iam_role.glue_role.arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn        = aws_lakeformation_resource.bucket-curated.arn
    catalog_id = var.owner_gobierno
  }
}

resource "aws_lakeformation_permissions" "data_product_location_access" {

  depends_on = [
    aws_lakeformation_resource.bucket-products
  ]
  principal   = aws_iam_role.glue_role.arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn        = aws_lakeformation_resource.bucket-products.arn
    catalog_id = var.owner_gobierno
  }
}

resource "aws_lakeformation_permissions" "data_product_location_access_viewers" {
  for_each = var.viewer_users_arn
  depends_on = [
    aws_lakeformation_resource.bucket-products
  ]
  principal   = each.key
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn        = aws_lakeformation_resource.bucket-products.arn
    catalog_id = var.owner_gobierno
  }
}

# Data lake permissions

# Default
resource "aws_lakeformation_permissions" "default_permissions_database" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["DESCRIBE", "CREATE_TABLE"]

  database {
    name       = aws_glue_catalog_database.aws_glue_catalog_db[1].name
    catalog_id = var.owner_ciencuadras
  }
}


resource "aws_lakeformation_permissions" "default_permissions_tables" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT"]

  table {
    database_name = aws_glue_catalog_database.aws_glue_catalog_db[1].name
    wildcard      = true
    catalog_id    = var.owner_ciencuadras
  }
}


# Rl- ciencuadras
## Glue
resource "aws_lakeformation_permissions" "rl_ciencuadras_permissions_tables" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["DROP"]

  database {
    name       = aws_glue_catalog_database.aws_glue_catalog_db[0].name
    catalog_id = var.owner_ciencuadras
  }
}
## Viewers
resource "aws_lakeformation_permissions" "rl_ciencuadras_permissions_viewers_database" {
  for_each    = var.viewer_users_arn
  principal   = each.key
  permissions = ["DESCRIBE"]

  database {
    name       = aws_glue_catalog_database.aws_glue_catalog_db[0].name
    catalog_id = var.owner_ciencuadras
  }
}

#LF tags expresions

resource "aws_lakeformation_permissions" "rol_permissions_LFTAGS_DATABASE" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["ALTER", "CREATE_TABLE", "DESCRIBE"]

  lf_tag_policy {

    resource_type = "DATABASE"
    expression {
      key    = var.area
      values = [var.value_ciencuadras]
    }
    catalog_id = var.owner_gobierno

  }
}

resource "aws_lakeformation_permissions" "rol_permissions_LFTAGS_TABLE" {
  principal   = aws_iam_role.glue_role.arn
  permissions = ["ALTER", "DELETE", "DESCRIBE", "DROP", "INSERT", "SELECT"]

  lf_tag_policy {

    resource_type = "TABLE"
    expression {
      key    = var.area
      values = [var.value_ciencuadras]
    }
    catalog_id = var.owner_gobierno

  }
}

resource "aws_lakeformation_permissions" "viewer_LFTAGS_DATABASE" {
  for_each    = var.viewer_users_arn
  principal   = each.key
  permissions = ["DESCRIBE"]

  lf_tag_policy {

    resource_type = "DATABASE"
    expression {
      key    = var.area
      values = [var.value_ciencuadras]
    }
    catalog_id = var.owner_gobierno

  }
}

resource "aws_lakeformation_permissions" "viewer_LFTAGS_TABLE" {
  for_each    = var.viewer_users_arn
  principal   = each.key
  permissions = ["DESCRIBE", "SELECT"]

  lf_tag_policy {

    resource_type = "TABLE"
    expression {
      key    = var.area
      values = [var.value_ciencuadras]
    }
    catalog_id = var.owner_gobierno

  }
}
