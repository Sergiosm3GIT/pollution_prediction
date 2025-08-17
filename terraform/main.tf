########################################
# 1) Enable required APIs
########################################
resource "google_project_service" "services" {
  for_each = toset([
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "storage.googleapis.com"
  ])
  service = each.key
}

########################################
# 2) Artifact Registry (ya existente) como data source
########################################
data "google_artifact_registry_repository" "repo" {
  location      = var.region
  repository_id = var.repo_name
  depends_on    = [google_project_service.services]
}

# Imagen que YA empujas (no se construye aquí)
locals {
  image_uri = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repo_name}/${var.image_name}:latest"

  preprocess_image_uri = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repo_name}/openaq-preprocess:latest"
}

########################################
# 3) Secret Manager (API key OpenAQ)
########################################
resource "google_secret_manager_secret" "openaq" {
  secret_id = "openaq-api-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.services]
}

resource "google_secret_manager_secret_version" "openaq_v" {
  secret      = google_secret_manager_secret.openaq.id
  secret_data = var.openaq_api_key
}

########################################
# 4) Service Account for Jobs + IAM
########################################
resource "google_service_account" "jobs_sa" {
  account_id   = "sa-openaq-jobs"
  display_name = "Service Account for OpenAQ Cloud Run Jobs"
}

# IAM a nivel proyecto
resource "google_project_iam_member" "ar_reader" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_project_iam_member" "secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.jobs_sa.email}"
}

########################################
# 5) GCS Bucket for data + IAM
########################################
resource "google_storage_bucket" "data" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  storage_class               = "STANDARD"

  versioning { enabled = true }

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 45 } # ajusta o elimina para prod
  }

  labels = {
    project = "pollution-prediction"
    layer   = "datalake"
    env     = "dev"
  }
}

resource "google_storage_bucket_iam_member" "bucket_write" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.jobs_sa.email}"
}

########################################
# 6) Cloud Run Job (usa la imagen :latest ya publicada)
########################################

resource "google_cloud_run_v2_job" "extract" {
  name     = "openaq-extract-job"
  location = var.region

  template {
    template {
      service_account = google_service_account.jobs_sa.email

      containers {
        image = local.image_uri

        # Env normales
        env {
          name  = "CITY"
          value = var.city
        }
        env {
          name  = "COORDINATES"
          value = var.coordinates
        }
        env {
          name  = "RADIUS_M"
          value = tostring(var.radius_m)
        }
        env {
          name  = "PARAMETERS"
          value = var.parameters
        }
        env {
          name  = "GCS_BUCKET"
          value = var.bucket_name
        }
        env {
          name  = "STATE_BLOB"
          value = var.state_blob
        }
        env {
          name  = "CADENCE_HOURS"
          value = tostring(var.cadence_hours)
        }
        env {
          name  = "SAFETY_OVERLAP_MIN"
          value = tostring(var.safety_overlap_min)
        }

        # Env desde Secret Manager
        env {
          name = "OPENAQ_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.openaq.name
              version = "latest"
            }
          }
        }
      }
    }
  }

  # Espera a que el bucket/iam esté listo
  depends_on = [
    google_storage_bucket_iam_member.bucket_write,
    google_project_iam_member.secret_accessor,
    google_project_iam_member.ar_reader
  ]
}

resource "google_cloud_run_v2_job" "preprocess" {
  name     = "openaq-preprocess-job"
  location = var.region

  template {
    template {
      service_account = google_service_account.jobs_sa.email

      containers {
        image = local.preprocess_image_uri

        env {
          name  = "CITY"
          value = var.city
        }
        env {
          name  = "GCS_BUCKET"
          value = var.bucket_name
        }
        env {
          name  = "PARAMETERS"
          value = var.parameters
        }
        env {
          name  = "THRESHOLD_PM25"
          value = "25"
        }
        # Si alguna vez quieres fijar PROC_DATE por plantilla:
        # env {
        #   name  = "PROC_DATE"
        #   value = "2025-08-17"
        # }
      }
    }
  }

  depends_on = [
    google_project_service.services,
    google_storage_bucket_iam_member.bucket_write,
    google_project_iam_member.ar_reader
  ]
}

# Permitir que nuestra SA invoque el Job (Scheduler usa OAuth con esta SA)
resource "google_cloud_run_v2_job_iam_member" "preprocess_invoker" {
  name     = google_cloud_run_v2_job.preprocess.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.jobs_sa.email}"
}

resource "google_cloud_run_v2_job_iam_member" "allow_job_run" {
  name     = google_cloud_run_v2_job.extract.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.jobs_sa.email}"
}


########################################
# 7) Cloud Scheduler (dispara el Job por cron)
########################################
resource "google_cloud_scheduler_job" "extract_every" {
  name      = "openaq-extract-every"
  schedule  = var.cron_expr
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    # <-- CAMBIAR ESTA LÍNEA
    uri = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.extract.name}:run"

    oauth_token {
      service_account_email = google_service_account.jobs_sa.email
    }
  }

  depends_on = [
    google_cloud_run_v2_job.extract,
    google_cloud_run_v2_job_iam_member.allow_job_run
  ]
}

resource "google_cloud_scheduler_job" "preprocess_after_extract" {
  name = "openaq-preprocess-after-extract"
  # 10 minutos después de cada múltiplo de 3h
  schedule  = "10 */3 * * *"
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.preprocess.name}:run"

    oauth_token {
      service_account_email = google_service_account.jobs_sa.email
    }
  }

  depends_on = [
    google_cloud_run_v2_job.preprocess,
    google_cloud_run_v2_job_iam_member.preprocess_invoker
  ]
}

########################################
# 8) Outputs
########################################
output "image_uri" { value = local.image_uri }
output "job_name" { value = google_cloud_run_v2_job.extract.name }
output "bucket_name" { value = google_storage_bucket.data.name }
output "preprocess_image_uri" { value = local.preprocess_image_uri }
output "preprocess_job_name" { value = google_cloud_run_v2_job.preprocess.name }
