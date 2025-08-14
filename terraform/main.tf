resource "google_storage_bucket" "test_bucket" {
  name                        = "sergio-tf-test-bucket-${random_id.suffix.hex}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

resource "random_id" "suffix" {
  byte_length = 2
}
