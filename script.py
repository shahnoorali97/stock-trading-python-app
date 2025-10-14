import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") 

LIMIT = 1000

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
tickers = []

# Check if the request was successful
if response.status_code != 200:
    print(f"API request failed with status code: {response.status_code}")
    print(f"Response: {response.text}")
    exit(1)

data = response.json()
print("Full response structure:")
print(data)

# Check if the response has the expected structure
if 'results' not in data:
    print("No 'results' key found in response. Available keys:")
    print(list(data.keys()))
    exit(1)

print(f"Next URL: {data.get('next_url', 'None')}")
for ticker in data['results']:
    tickers.append(ticker)

while 'next_url' in data and data['next_url']:
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    
    if response.status_code != 200:
        print(f"Next page request failed with status code: {response.status_code}")
        break
        
    data = response.json()
    print("Next page response:")
    print(data)
    
    if 'results' not in data:
        print("No 'results' key found in next page response")
        break
        
    for ticker in data['results']:
        tickers.append(ticker)

print(f"Total tickers collected: {len(tickers)}")

# Write tickers to CSV file
csv_filename = 'tickers.csv'
fieldnames = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active', 'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc']

with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for ticker in tickers:
        # Ensure all fields are present, use empty string for missing fields
        row = {}
        for field in fieldnames:
            row[field] = ticker.get(field, '')
        writer.writerow(row)

print(f"Tickers written to {csv_filename}")

example_ticker = {'ticker': 'HSAI', 
    'name': 'Hesai Group American Depositary Share, each ADS represents one Class B ordinary share', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'XNAS', 
    'type': 'CS', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001861737', 
    'composite_figi': 'BBG01CCYDD47', 
    'share_class_figi': 'BBG01CCYDDZ3', 
    'last_updated_utc': '2025-10-14T15:02:57.620444069Z'}
