# --------------------------------------
# S3
# --------------------------------------

module "buckets_s3" {
  source                  = "git::https://github.com/segurosbolivar/libreria-modulos-comunes-infra.git//modules/s3?ref=v2.4.13"
  for_each                = local.buckets_s3
  name                    = "${var.proyecto}-${var.dominio}-${var.ambiente}-${each.key}-s3-bk"
  acl                     = null
  tags                    = var.tags
  attach_policy           = each.value["attach_policy"] ? true : false
  policy                  = each.value["attach_policy"] ? data.template_file.json_template_policy_s3[each.key].rendered : null
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# resource "aws_s3_bucket_public_access_block" "s3_bucket" {
#   for_each = module.buckets_s3
#   bucket   = each.value.id

#   block_public_acls       = true
#   block_public_policy     = true
#   ignore_public_acls      = true
#   restrict_public_buckets = true

#   depends_on = [module.buckets_s3]
# }

resource "aws_s3_object" "dependencies" {
  for_each = fileset("./files/", "**")
  bucket   = module.buckets_s3["dependencies"].name
  key      = each.value
  source   = "./files/${each.value}"
  etag     = filemd5("./files/${each.value}")
}