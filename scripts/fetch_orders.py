import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load API Keys
load_dotenv()
api_key = os.getenv('WC_API_KEY')
api_secret = os.getenv('WC_API_SECRET')
api_url = os.getenv('WC_API_URL')


params = {
    'per_page': 100,
    'page': 1
}

all_orders = []

while True:
    print(f"Fetching page {params['page']}...")
    response = requests.get(api_url, auth=HTTPBasicAuth(
        api_key, api_secret), params=params)

    if response.status_code != 200:
        print(f"Error {response.status_code}")
        break

    page_data = response.json()
    if not page_data:
        break

    all_orders.extend(page_data)
    params["page"] += 1

df = pd.json_normalize(all_orders)

# Create output folder to save raw_data
os.makedirs("data", exist_ok=True)
df.to_csv("data/raw_orders.csv", index=False)
print("Orders saved to data/raw_orders.csv")