terraform {
  backend "gcs" {
    bucket = "fraud-prediction-499405-tfstate"
    prefix = "terraform/state"
  }
}
