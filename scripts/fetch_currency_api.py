import requests
import json
import os
from datetime import date

# 🔑 Replace with your API key
API_KEY = os.getenv("29de8b594a88cb2b8841c0c3")


# API endpoint
url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"

# Create folder for today
today = str(date.today())
folder = f"data/raw/api_currency/{today}"
os.makedirs(folder, exist_ok=True)

# Fetch API data
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    # Save JSON response
    with open(f"{folder}/rates.json", "w") as f:
        json.dump(data, f, indent=4)
    print(f"✅ API data saved for {today}")
else:
    print(f"⚠️ API request failed! Status code: {response.status_code}")
