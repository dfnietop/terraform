terraform {
  backend "s3" {
    encrypt = true
    bucket  = "terraform-provider-versioning"
    key     = "ciencuadras-dataoperativa-infra/terraform.tfstate"
    region  = "us-east-1"
  }
}
