import pandas as pd
import json
import os
from datetime import date

# Load transactions CSV
transactions_path = "data/raw/transactions.csv"
df = pd.read_csv(transactions_path)


print(f"Original records: {len(df)}")
print(f"📥 Loaded {len(df)} transaction records")


# Standardize column names
df.columns = df.columns.str.lower().str.replace(" ", "_")

# Remove duplicates
df = df.drop_duplicates()

print(f"Records after removing duplicates: {len(df)}")
print(f"🧹 After cleaning: {len(df)} records remain")

# Load today's currency rates
today = str(date.today())
rates_path = f"data/raw/api_currency/{today}/rates.json"

with open(rates_path, "r") as f:
    rates_data = json.load(f)

rates = rates_data["conversion_rates"]

# Convert to USD
def convert_to_usd(row):
    currency = row["currency"]
    amount = row["amount"]

    if currency == "USD":
        return amount

    if currency in rates:
        return amount / rates[currency]

    return None

df["amount_in_usd"] = df.apply(convert_to_usd, axis=1)

# Save cleaned data
output_folder = f"data/processed/ingest_date={today}"
os.makedirs(output_folder, exist_ok=True)

output_path = f"{output_folder}/clean_transactions.csv"
df.to_csv(output_path, index=False)

print("✅ Transformation complete!")
print(f"Clean file saved to: {output_path}")
missing = df["amount_in_usd"].isnull().sum()
print(f"⚠️ Missing conversions: {missing}")