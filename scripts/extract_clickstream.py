import pandas as pd

file_path = "data/raw/clickstream.csv"

try:
    chunk_number = 1

    for chunk in pd.read_csv(file_path, chunksize=50000):
        print(f"Chunk {chunk_number} loaded with {len(chunk)} records")
        chunk_number += 1

    print("✅ Extraction completed successfully!")

except FileNotFoundError:
    print("⚠️ File not found. Check if clickstream.csv is in data/raw folder.")
