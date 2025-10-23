import requests

ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0MkJOVUMiLCJqdGkiOiI2OGUzNTc4ZWNlOWM3OTZhZmM4YzQ4MzciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1OTcyOTU1MCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzU5Nzg4MDAwfQ.FMx7rEIjneyXMj7HMYpC9JY-3gKtGU0-jWNtVnn8RRE'
INSTRUMENT_KEY = "NSE_EQ|INE511C01022"  # POONAWALLA FinCorp
UNIT = "minutes"  # minutes, hours, or days
INTERVAL = 15     # valid values: 1, 3, 5, 10, 15, 30, 60, etc.

url = f"https://api.upstox.com/v3/historical-candle/intraday/{INSTRUMENT_KEY}/{UNIT}/{INTERVAL}"

headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

response = requests.get(url, headers=headers)
print("Status Code:", response.status_code)
print(response.json())