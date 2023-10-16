 
module "sns_monitoring" {
  source                         = "git::https://github.com/segurosbolivar/libreria-modulos-comunes-infra.git//modules/integration/sns?ref=v2.4.0"
  stack_id                       = var.dominio
  layer                          = var.proyecto
  region                         = var.region
  tags                           = var.tags
  create_delivery_status_logging = false
  sns_topic_data = {
    name                        = "${var.ambiente}-monitoring"
    fifo_topic                  = false
    content_based_deduplication = false
    server_side_encryption      = false
    delivery_policy             = file("./policies/snsdeliverpolicy.json")
    policy                      = data.template_file.json_template_policy_sns.rendered
  }
}

resource "aws_sns_topic_subscription" "email_notifications_stepfunctions_errors" {
    for_each = var.monitoring_emails
    topic_arn = module.sns_monitoring.sns_topic_arn
    endpoint = each.value
    protocol = "email"
}