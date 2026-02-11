from google.cloud import storage
from datetime import date
import os

# Replace with your bucket name
bucket_name = "anjali-pendly-1"

today = str(date.today())

file_path = f"data/processed/ingest_date={today}/clean_transactions.csv"

destination_blob = f"transactions/ingest_date={today}/clean_transactions.csv"

# Connect to GCS
client = storage.Client()
bucket = client.bucket(bucket_name)
blob = bucket.blob(destination_blob)

# Upload file
blob.upload_from_filename(file_path)

print("✅ File uploaded to Google Cloud Storage!")
print(f"📂 Uploaded to: {destination_blob}")
