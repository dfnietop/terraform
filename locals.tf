locals {
  buckets_s3 = {
    raw = {
      acl           = "private"
      attach_policy = true
      policy_path   = "./policies/s3-raw-policy.json"
    }
    curated = {
      acl           = "private"
      attach_policy = true
      policy_path   = "./policies/s3-curated-policy.json"
    }
    dependencies = {
      acl           = "private"
      attach_policy = true
      policy_path   = "./policies/s3-dependencies-policy.json"
    }
    products = {
      acl           = "private"
      attach_policy = true
      policy_path   = "./policies/s3-products-policy.json"
    }
  }
  # Glue Jobs
  processing_glue_jobs = {
    cdc = {
      capaalmacenamientoorigen  = "raw"
      capaalmacenamientodestino = "curated"
      propositoprocesamiento    = "cdc"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibrary.py",
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/cdc.py"
      glue_version      = "4.0"
      number_of_workers = "2"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    }
    full_load = {
      capaalmacenamientoorigen  = "raw"
      capaalmacenamientodestino = "curated"
      propositoprocesamiento    = "full-load"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibrary.py",
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/full_load.py"
      glue_version      = "4.0"
      number_of_workers = "7"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    }
    inmuebles = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "inmuebles"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibrary.py",
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/producto_inmueble.py"
      glue_version      = "4.0"
      number_of_workers = "7"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    }
    usuarios = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "usuarios"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibrary.py",
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/producto_usuarios.py"
      glue_version      = "4.0"
      number_of_workers = "7"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    },
    leads = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "leads"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibrary.py",
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/leads.py"
      glue_version      = "4.0"
      number_of_workers = "7"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    },
    user_plans = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "user_plans"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/user_plans.py"
      glue_version      = "4.0"
      number_of_workers = "2"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    },
    perfilamiento_cliente = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "perfilamiento_cliente"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/perfilamiento_cliente.py"
      glue_version      = "4.0"
      number_of_workers = "2"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    },
    ofertas = {
      capaalmacenamientoorigen  = "curated"
      capaalmacenamientodestino = "products"
      propositoprocesamiento    = "ofertas"
      fuente                    = "ciencuadras"
      libraries = [
        "glueAssets/libraries/glueLibraryV2.py",
      ]
      file_name         = "glueAssets/glueScripts/ofertas.py"
      glue_version      = "4.0"
      number_of_workers = "2"
      format            = "parquet"
      extra_parameters = {
        "--additional-python-modules" = "jsonschema"
      }
    }
  }
  # quicksight = {
  #   oferta_demanda = {
  #     dataset_description = jsondecode(file("quicksight/oferta_demanda/dataset_description.json"))
  #     template_description = jsondecode(file("quicksight/inventario_leads/template_description.json"))
  #   }
  # }
}



data "aws_caller_identity" "current" {}

data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}
data "template_file" "json_template_policy_s3" {
  for_each = local.buckets_s3
  template = file("${each.value["policy_path"]}")
  vars = {
    owner_gobierno    = var.owner_gobierno
    owner_ciencuadras = var.owner_ciencuadras
    dominio           = var.dominio
    ambiente          = var.ambiente
    proyecto          = var.proyecto
  }
}

data "template_file" "json_template_policy_dms" {
  template = file("./policies/dmspolicy.json")
  vars = {
    owner_gobierno    = var.owner_gobierno
    owner_ciencuadras = var.owner_ciencuadras
    dominio           = var.dominio
    ambiente          = var.ambiente
    proyecto          = var.proyecto
  }
}

data "template_file" "json_template_policy_glue" {
  template = file("./policies/gluepolicy.json")
  vars = {
    owner_gobierno    = var.owner_gobierno
    owner_ciencuadras = var.owner_ciencuadras
    dominio           = var.dominio
    ambiente          = var.ambiente
    proyecto          = var.proyecto
  }
}

data "template_file" "json_template_policy_sf" {
  template = file("./policies/sf-policy.json")
  vars = {
    owner_gobierno    = var.owner_gobierno
    owner_ciencuadras = var.owner_ciencuadras
    dominio           = var.dominio
    ambiente          = var.ambiente
    proyecto          = var.proyecto
  }
}

data "template_file" "json_template_policy_scheduler" {
  template = file("./policies/scheduler-policy.json")
  vars = {
    owner_gobierno    = var.owner_gobierno
    owner_ciencuadras = var.owner_ciencuadras
    dominio           = var.dominio
    ambiente          = var.ambiente
    proyecto          = var.proyecto
  }
}

data "template_file" "json_template_definition_ciencuadras_sf" {
  template = file("${var.sfn_statemachines.ciencuadras.definition_path}")
  vars = {
    fl_job_name          = "${var.ambiente}-${local.processing_glue_jobs["full_load"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["full_load"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["full_load"]["propositoprocesamiento"]}-${local.processing_glue_jobs["full_load"]["fuente"]}-glue-job"
    cdc_job_name         = "${var.ambiente}-${local.processing_glue_jobs["cdc"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["cdc"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["cdc"]["propositoprocesamiento"]}-${local.processing_glue_jobs["cdc"]["fuente"]}-glue-job"
    inmuebles_job_name   = "${var.ambiente}-${local.processing_glue_jobs["inmuebles"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["inmuebles"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["inmuebles"]["propositoprocesamiento"]}-${local.processing_glue_jobs["inmuebles"]["fuente"]}-glue-job"
    usuarios_job_name    = "${var.ambiente}-${local.processing_glue_jobs["usuarios"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["usuarios"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["usuarios"]["propositoprocesamiento"]}-${local.processing_glue_jobs["usuarios"]["fuente"]}-glue-job"
    leads_job_name       = "${var.ambiente}-${local.processing_glue_jobs["leads"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["leads"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["leads"]["propositoprocesamiento"]}-${local.processing_glue_jobs["leads"]["fuente"]}-glue-job"
    user_plans_job_name  = "${var.ambiente}-${local.processing_glue_jobs["user_plans"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["user_plans"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["user_plans"]["propositoprocesamiento"]}-${local.processing_glue_jobs["user_plans"]["fuente"]}-glue-job"
    perfilamiento_cliente_job_name ="${var.ambiente}-${local.processing_glue_jobs["perfilamiento_cliente"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["perfilamiento_cliente"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["perfilamiento_cliente"]["propositoprocesamiento"]}-${local.processing_glue_jobs["perfilamiento_cliente"]["fuente"]}-glue-job"
    ofertas_job_name = "${var.ambiente}-${local.processing_glue_jobs["ofertas"]["capaalmacenamientoorigen"]}-${local.processing_glue_jobs["ofertas"]["capaalmacenamientodestino"]}-${local.processing_glue_jobs["ofertas"]["propositoprocesamiento"]}-${local.processing_glue_jobs["ofertas"]["fuente"]}-glue-job"
    replication_task_arn = module.database_migration_service.replication_tasks.mysql_fl_cdc.replication_task_arn
  }
}

data "template_file" "json_template_policy_sns" {
  template = file("./policies/snspolicy.json")
  vars = {
    owner_id = var.owner_ciencuadras
  }
}