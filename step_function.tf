# recursos para step functions
module "sfn_ciencuadras" {
  source      = "git::https://github.com/segurosbolivar/libreria-modulos-comunes-infra.git//modules/compute/step_functions?ref=v2.6.4"
  stack_id    = var.ambiente
  layer       = var.proyecto
  name        = "${var.proyecto}-${var.dominio}-${var.ambiente}-ciencuadras-step-sm"
  tags        = var.tags
  definition  = data.template_file.json_template_definition_ciencuadras_sf.rendered
  policy_json = data.template_file.json_template_policy_sf.rendered
  region      = var.region

  depends_on = [aws_glue_job.glue_job, module.database_migration_service]
}
