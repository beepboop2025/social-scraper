"""
Helper script to get Twitter cookies manually
Run this and follow the instructions
"""

import json
import os
from pathlib import Path

print("""
=== Twitter Cookie Setup ===

Since Twitter has Cloudflare protection, you need to manually get cookies:

Method 1: Browser Extension (Easiest)
1. Login to twitter.com in your browser
2. Install "Cookie-Editor" extension (Chrome/Firefox)
3. Click the extension icon
4. Click "Export" → "JSON"
5. Save the content to: /Users/mrinal/social_scraper/cookies.json

Method 2: Manual Cookie Creation
Paste your auth_token below (from browser dev tools):
""")

# Create sample cookies structure
sample_cookies = [
    {
        "name": "auth_token",
        "value": "YOUR_AUTH_TOKEN_HERE",
        "domain": ".twitter.com",
        "path": "/"
    },
    {
        "name": "ct0",
        "value": "YOUR_CT0_TOKEN_HERE",
        "domain": ".twitter.com",
        "path": "/"
    }
]

print("\nSample cookies.json structure:")
print(json.dumps(sample_cookies, indent=2))

print("""
\nTo get these values:
1. Open twitter.com in browser (logged in)
2. Press F12 → Application/Storage → Cookies
3. Copy values for 'auth_token' and 'ct0'
4. Paste them below:
""")

auth_token = input("auth_token: ").strip()
ct0 = input("ct0: ").strip()

if auth_token and ct0:
    cookies = [
        {"name": "auth_token", "value": auth_token, "domain": ".twitter.com", "path": "/"},
        {"name": "ct0", "value": ct0, "domain": ".twitter.com", "path": "/"}
    ]
    
    fd = os.open("cookies.json", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(cookies, f, indent=2)

    print("\n✓ cookies.json created with restricted permissions (0600)!")
else:
    print("\n✗ Missing values. Please try again.")
