"""Quick test: send a hello to DeepSeek and print the reply."""

import os
import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
if not API_KEY:
    print("ERROR: DEEPSEEK_API_KEY not set in .env")
    exit(1)

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type":  "application/json",
}
payload = {
    "model":      "deepseek-chat",
    "messages":   [{"role": "user", "content": "Say hello in one sentence."}],
    "max_tokens": 128,
}

print("Sending request to DeepSeek...")
resp = httpx.post(
    "https://api.deepseek.com/chat/completions",
    headers=headers,
    json=payload,
    timeout=30,
)

print(f"Status: {resp.status_code}")
print(f"Raw response: {resp.text}")
