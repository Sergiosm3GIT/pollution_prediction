project_id = "calm-energy-468819-u7"
region     = "europe-west2"
repo_name  = "openaq-repo"
image_name = "openaq-extract"

bucket_name     = "pollution-data-mlops"
bucket_location = "EUROPE-WEST2"

openaq_api_key     = "fd1d7ab8d412bea2367cc35c10feb9f6f47be9d27cac3ab2eef59cac317d7d8a"
city               = "Santiago"
coordinates        = "-33.4489,-70.6693"
radius_m           = 25000
parameters         = "pm25,pm10,no2"
state_blob         = "state/openaq_extract_state.json"
cadence_hours      = 3
safety_overlap_min = 15

cron_expr = "0 */3 * * *"
