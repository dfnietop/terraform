
resource "aws_scheduler_schedule" "schedule_ciencuadras" {
  name       = "${var.proyecto}-${var.dominio}-${var.ambiente}-ciencuadras-bridge-sc"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }
  start_date          = var.sfn_statemachines.ciencuadras.start_date
  end_date            = var.sfn_statemachines.ciencuadras.end_date
  schedule_expression = var.sfn_statemachines.ciencuadras.schedule_expression

  target {
    arn      = module.sfn_ciencuadras.arn
    role_arn = aws_iam_role.bridge_role.arn

    input = jsonencode({
      Commet = "Inicio flujo step functions ciencuadras"
    })
  }
}

resource "aws_iam_role" "bridge_role" {
  name = "${var.proyecto}-${var.dominio}-${var.ambiente}-eventbridge-scheduler-iam-rol"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_policy" "bridge_policy" {
  name   = "${var.proyecto}-${var.dominio}-${var.ambiente}-eventbridge-scheduler-iam-pl"
  policy = data.template_file.json_template_policy_scheduler.rendered
}

resource "aws_iam_role_policy_attachment" "bridge_policy_attachment" {
  role       = aws_iam_role.bridge_role.name
  policy_arn = aws_iam_policy.bridge_policy.arn
}