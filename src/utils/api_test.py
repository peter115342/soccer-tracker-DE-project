import os
import requests

api_key = os.environ.get("API_FOOTBALL_KEY")

if not api_key:
    raise ValueError("API_FOOTBALL_KEY environment variable is not set")

url = "https://v3.football.api-sports.io/status"

headers = {
    "x-apisports-key": api_key,
    "x-apisports-host": "v3.football.api-sports.io"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    print("API call successful!")
    print("Response JSON:")
    print(response.json())
else:
    print(f"API call failed with status code: {response.status_code}")
    print("Response text:")
    print(response.text)
