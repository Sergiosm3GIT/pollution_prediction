resource "google_storage_bucket" "extract_bucket" {
  name                        = "sergio-tf-extract-bucket"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "random_id" "suffix" {
  byte_length = 2
}
