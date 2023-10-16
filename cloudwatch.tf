
resource "aws_cloudwatch_metric_alarm" "monitoring_sfn_ciencuadras" {
  alarm_name          = "${var.proyecto}-${var.dominio}-${var.ambiente}-sfn-ciencuadras"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 1800
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Failed executions Step functions ciencuadras pipeline"
  actions_enabled     = "true"
  alarm_actions       = [module.sns_monitoring.sns_topic_arn]
  ok_actions          = [module.sns_monitoring.sns_topic_arn]
  dimensions = {
    StateMachineArn = module.sfn_ciencuadras.arn
  }
  treat_missing_data = "ignore"
  unit = "Count"
}