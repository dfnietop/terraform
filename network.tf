
# ------------------------------------#
# VPC                                 #
# ------------------------------------# 

module "network" {
  source                  = "git::https://github.com/segurosbolivar/libreria-modulos-comunes-infra.git//modules/network?ref=v1.0.5"
  name                    = "${var.proyecto}-${var.dominio}-${var.ambiente}-vpc"
  stack_id                = var.ambiente
  layer                   = var.dominio
  tags                    = var.tags
  vpc_cidr                = var.vpc_cidr
  azs                     = var.azs
  private_subnets         = var.private_subnets
  public_subnets          = var.public_subnets
  transit_gateway_id      = var.transit_gateway_id
  peering_mongoatlas_id   = var.peering
  peering_mongoatlas_cird = "10.50.0.0/16"
}
# --------------------------------------#
# Grupos de seguridad                   #
# --------------------------------------#
resource "aws_security_group" "sg_general" {
  name        = "sg_${var.dominio}_${var.ambiente}"
  description = "Allow all ports within the VPC, and browsing from the outside"
  vpc_id      = module.network.vpc_id
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [module.network.vpc_cidr]
    description = "Anyport from VPC"
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = {
    Name   = "sg_${var.dominio}_${var.ambiente}"
    Source = "Terraform"
  }
}
# --------------------------------------#
# Grupo de seguridad DMS                #
# --------------------------------------#
resource "aws_security_group" "dms_sg" {
  name        = "sg_allow_migration_${var.dominio}_${var.ambiente}"
  description = "allow_migration"
  vpc_id      = module.network.vpc_id
  # @WARINNG
  ingress {
    description      = "Allow all"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
  egress {
    description      = "Allow all"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = merge(var.tags, { Name = "sg_allow_migration_${var.dominio}_${var.ambiente}" })
}
# END

# --------------------------------------#
# VPC Flow Logs                         #
# --------------------------------------#

resource "aws_flow_log" "vpc_flow_logs" {
  iam_role_arn             = aws_iam_role.vpc_flow_logs_role.arn
  log_destination          = aws_cloudwatch_log_group.vpc_flow_logs_log_group.arn
  traffic_type             = "ALL"
  vpc_id                   = module.network.vpc_id
  log_format               = "$${version} $${account-id} $${interface-id} $${srcaddr} $${dstaddr} $${srcport} $${dstport} $${protocol} $${packets} $${bytes} $${start} $${end} $${action} $${log-status} $${traffic-path}"
  max_aggregation_interval = 60
}

resource "aws_cloudwatch_log_group" "vpc_flow_logs_log_group" {
  name              = "/VPC-flow/do-ciencuadras-dev-vpc"
  retention_in_days = 30
}


resource "aws_iam_role" "vpc_flow_logs_role" {
  name = "${var.proyecto}-${var.dominio}-${var.ambiente}-vpc-flow-logs-iam-rol"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "vpc-flow-logs.amazonaws.com"
        }
      },
    ]
  })

  inline_policy {
    name = "${var.proyecto}-${var.dominio}-${var.ambiente}-vpc-flow-logs-iam-pl"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:DescribeLogGroups",
            "logs:DescribeLogStreams",
          ]
          Resource = "*"
        },
      ]
    })
  }
}