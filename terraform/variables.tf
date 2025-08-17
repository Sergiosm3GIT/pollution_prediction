variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "repo_name" {
  type = string
}

variable "image_name" {
  type = string
}

variable "bucket_name" {
  type = string
}

variable "bucket_location" {
  type    = string
  default = "EUROPE-WEST2"
}

variable "openaq_api_key" {
  type      = string
  sensitive = true
}

variable "city" {
  type    = string
  default = "Santiago"
}

variable "coordinates" {
  type    = string
  default = "-33.4489,-70.6693"
}

variable "radius_m" {
  type    = number
  default = 25000
}

# CSV
variable "parameters" {
  type    = string
  default = "pm25,pm10,no2"
}

variable "state_blob" {
  type    = string
  default = "state/openaq_extract_state.json"
}

variable "cadence_hours" {
  type    = number
  default = 3
}

variable "safety_overlap_min" {
  type    = number
  default = 15
}

variable "cron_expr" {
  type    = string
  default = "0 */3 * * *"
}

/* Si ya no usas el build “one-shot” dentro de Terraform, elimina estos.
variable "dockerfile_path" {
  type    = string
  default = "../../jobs/extract.Dockerfile"
}

variable "docker_context" {
  type    = string
  default = "../../"
}
*/
