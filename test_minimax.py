"""Quick test: send a hello to MiniMax-M2.7 and print the reply."""

import os
import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MINIMAX_API_KEY", "")
if not API_KEY:
    print("ERROR: MINIMAX_API_KEY not set in .env")
    exit(1)

headers = {
    "x-api-key":         API_KEY,
    "content-type":      "application/json",
    "anthropic-version": "2023-06-01",
}
payload = {
    "model":      "MiniMax-M2.7",
    "messages":   [{"role": "user", "content": "Say hello in one sentence."}],
    "max_tokens": 128,
}

print("Sending request to MiniMax-M2.7...")
resp = httpx.post(
    "https://api.minimax.io/anthropic/v1/messages",
    headers=headers,
    json=payload,
    timeout=30,
)

print(f"Status: {resp.status_code}")
print(f"Raw response: {resp.text}")
