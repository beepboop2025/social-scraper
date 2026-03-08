"""Catalog of free/easy-to-get API keys for economic & financial data.

Each entry describes: the API, its free tier limits, signup URL,
how to get a key, what env var it maps to, and a test endpoint.
"""

CATALOG: dict[str, dict] = {
    # ── Economic Data APIs ─────────────────────────────────────
    "fred": {
        "name": "FRED (Federal Reserve Economic Data)",
        "provider": "Federal Reserve Bank of St. Louis",
        "signup_url": "https://fred.stlouisfed.org/docs/api/api_key.html",
        "free_tier": "120 requests/minute, unlimited data",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://fred.stlouisfed.org/docs/api/api_key.html",
            "Click 'Request API Key'",
            "Create a free account or sign in",
            "Your API key is displayed immediately",
        ],
        "env_var": "FRED_API_KEY",
        "test_endpoint": "https://api.stlouisfed.org/fred/series?series_id=GDP&api_key={key}&file_type=json",
        "test_status": 200,
        "category": "economic_data",
        "used_by": ["collectors.fred_api.FredCollector"],
        "priority": "high",
    },

    "alpha_vantage": {
        "name": "Alpha Vantage (Stock & Forex Data)",
        "provider": "Alpha Vantage Inc.",
        "signup_url": "https://www.alphavantage.co/support/#api-key",
        "free_tier": "25 requests/day, 5 req/min",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://www.alphavantage.co/support/#api-key",
            "Fill in your name, email, and use case",
            "Click 'Get Free API Key'",
            "Key is displayed and emailed to you",
        ],
        "env_var": "ALPHA_VANTAGE_API_KEY",
        "test_endpoint": "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={key}&datatype=json",
        "test_status": 200,
        "test_json_key": "Time Series (Daily)",
        "category": "market_data",
        "used_by": ["scrapers.web_scraper"],
        "priority": "medium",
    },

    "finnhub": {
        "name": "Finnhub (Real-time Stock & Crypto)",
        "provider": "Finnhub.io",
        "signup_url": "https://finnhub.io/register",
        "free_tier": "60 API calls/minute, real-time US stocks",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://finnhub.io/register",
            "Sign up with email",
            "API key is on the dashboard immediately",
        ],
        "env_var": "FINNHUB_API_KEY",
        "test_endpoint": "https://finnhub.io/api/v1/quote?symbol=AAPL&token={key}",
        "test_status": 200,
        "test_json_key": "c",
        "category": "market_data",
        "used_by": ["scrapers.web_scraper", "connectors.dragonscope"],
        "priority": "high",
    },

    "polygon": {
        "name": "Polygon.io (US Stocks, Options, Forex, Crypto)",
        "provider": "Polygon.io",
        "signup_url": "https://polygon.io/dashboard/signup",
        "free_tier": "5 API calls/minute, delayed data, unlimited history",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://polygon.io/dashboard/signup",
            "Create account with email",
            "Free API key is on the dashboard",
        ],
        "env_var": "POLYGON_API_KEY",
        "test_endpoint": "https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey={key}",
        "test_status": 200,
        "test_json_key": "results",
        "category": "market_data",
        "used_by": ["connectors.dragonscope"],
        "priority": "medium",
    },

    # ── News APIs ──────────────────────────────────────────────
    "newsapi": {
        "name": "NewsAPI.org (Global News Search)",
        "provider": "NewsAPI.org",
        "signup_url": "https://newsapi.org/register",
        "free_tier": "100 requests/day, 1-month-old articles",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://newsapi.org/register",
            "Sign up with name and email",
            "API key shown immediately + emailed",
        ],
        "env_var": "NEWSAPI_KEY",
        "test_endpoint": "https://newsapi.org/v2/top-headlines?country=us&pageSize=1&apiKey={key}",
        "test_status": 200,
        "test_json_key": "articles",
        "category": "news",
        "used_by": ["scrapers.rss_scraper", "collectors.rss_feeds"],
        "priority": "high",
    },

    "newsdata": {
        "name": "NewsData.io (News from 80+ Countries)",
        "provider": "NewsData.io",
        "signup_url": "https://newsdata.io/register",
        "free_tier": "200 credits/day, latest news + archive",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://newsdata.io/register",
            "Sign up with email",
            "API key on dashboard",
        ],
        "env_var": "NEWSDATA_API_KEY",
        "test_endpoint": "https://newsdata.io/api/1/latest?apikey={key}&language=en&size=1",
        "test_status": 200,
        "test_json_key": "results",
        "category": "news",
        "used_by": ["scrapers.rss_scraper"],
        "priority": "medium",
    },

    "gnews": {
        "name": "GNews (Google News Aggregator)",
        "provider": "GNews.io",
        "signup_url": "https://gnews.io/register",
        "free_tier": "100 requests/day",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://gnews.io/register",
            "Sign up with email",
            "API key shown on dashboard",
        ],
        "env_var": "GNEWS_API_KEY",
        "test_endpoint": "https://gnews.io/api/v4/top-headlines?lang=en&max=1&apikey={key}",
        "test_status": 200,
        "test_json_key": "articles",
        "category": "news",
        "used_by": ["scrapers.rss_scraper"],
        "priority": "medium",
    },

    "worldnewsapi": {
        "name": "World News API (Sentiment + NLP)",
        "provider": "WorldNewsAPI.com",
        "signup_url": "https://worldnewsapi.com/register",
        "free_tier": "500 requests/month",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://worldnewsapi.com/register",
            "Sign up",
            "API key on dashboard",
        ],
        "env_var": "WORLDNEWS_API_KEY",
        "test_endpoint": "https://api.worldnewsapi.com/search-news?text=economy&number=1&api-key={key}",
        "test_status": 200,
        "test_json_key": "news",
        "category": "news",
        "used_by": ["scrapers.rss_scraper"],
        "priority": "low",
    },

    # ── Crypto & Forex ─────────────────────────────────────────
    "coingecko": {
        "name": "CoinGecko (Crypto Market Data)",
        "provider": "CoinGecko",
        "signup_url": "https://www.coingecko.com/en/api/pricing",
        "free_tier": "30 calls/min, no key needed (demo key for higher limits)",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://www.coingecko.com/en/api/pricing",
            "Sign up for Demo plan (free)",
            "Get your Demo API key from dashboard",
            "Or use without key at lower rate limits",
        ],
        "env_var": "COINGECKO_API_KEY",
        "test_endpoint": "https://api.coingecko.com/api/v3/ping",
        "test_status": 200,
        "test_json_key": "gecko_says",
        "category": "crypto",
        "used_by": ["connectors.dragonscope"],
        "priority": "medium",
    },

    "exchangerate": {
        "name": "ExchangeRate-API (Currency Exchange Rates)",
        "provider": "ExchangeRate-API.com",
        "signup_url": "https://www.exchangerate-api.com/",
        "free_tier": "1500 requests/month, 50+ currencies",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://www.exchangerate-api.com/",
            "Enter email for free plan",
            "API key is emailed to you",
        ],
        "env_var": "EXCHANGERATE_API_KEY",
        "test_endpoint": "https://v6.exchangerate-api.com/v6/{key}/latest/USD",
        "test_status": 200,
        "test_json_key": "conversion_rates",
        "category": "forex",
        "used_by": ["connectors.liquifi"],
        "priority": "medium",
    },

    # ── India-specific ─────────────────────────────────────────
    "data_gov_in": {
        "name": "data.gov.in (Indian Government Open Data)",
        "provider": "Government of India",
        "signup_url": "https://data.gov.in/user/register",
        "free_tier": "Unlimited, all government datasets",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://data.gov.in/user/register",
            "Register with email",
            "Go to 'My Account' > 'API Keys'",
            "Generate a new API key",
        ],
        "env_var": "DATA_GOV_API_KEY",
        "test_endpoint": "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070?api-key={key}&format=json&limit=1",
        "test_status": 200,
        "category": "economic_data",
        "used_by": ["collectors.data_gov_in.DataGovCollector"],
        "priority": "high",
    },

    # ── Social Platforms ───────────────────────────────────────
    "reddit": {
        "name": "Reddit API (Posts & Comments)",
        "provider": "Reddit Inc.",
        "signup_url": "https://www.reddit.com/prefs/apps",
        "free_tier": "60 requests/min with OAuth, 10/min without",
        "signup_method": "manual",
        "signup_steps": [
            "Go to https://www.reddit.com/prefs/apps",
            "Click 'create another app...'",
            "Choose 'script' type",
            "Name: econscraper, Redirect URI: http://localhost:8080",
            "Client ID is under the app name, Client Secret is labeled 'secret'",
        ],
        "env_var": "REDDIT_CLIENT_ID",
        "env_vars": {"REDDIT_CLIENT_ID": "", "REDDIT_CLIENT_SECRET": ""},
        "test_endpoint": "https://www.reddit.com/r/wallstreetbets/top.json?limit=1&t=day",
        "test_status": 200,
        "category": "social",
        "used_by": ["scrapers.reddit_scraper"],
        "priority": "high",
    },

    "youtube": {
        "name": "YouTube Data API v3",
        "provider": "Google",
        "signup_url": "https://console.developers.google.com/apis/api/youtube.googleapis.com",
        "free_tier": "10,000 units/day (search = 100 units each)",
        "signup_method": "manual",
        "signup_steps": [
            "Go to https://console.cloud.google.com/",
            "Create a project (or select existing)",
            "Enable 'YouTube Data API v3'",
            "Go to Credentials > Create API Key",
            "Optionally restrict the key to YouTube Data API only",
        ],
        "env_var": "YOUTUBE_API_KEY",
        "test_endpoint": "https://www.googleapis.com/youtube/v3/search?part=snippet&q=economy&maxResults=1&key={key}",
        "test_status": 200,
        "test_json_key": "items",
        "category": "social",
        "used_by": ["scrapers.youtube_scraper"],
        "priority": "medium",
    },

    "github": {
        "name": "GitHub REST API (Repos, Issues, Releases)",
        "provider": "GitHub / Microsoft",
        "signup_url": "https://github.com/settings/tokens",
        "free_tier": "5000 requests/hour with token, 60/hour without",
        "signup_method": "manual",
        "signup_steps": [
            "Go to https://github.com/settings/tokens",
            "Click 'Generate new token (classic)'",
            "Select scopes: public_repo (minimum)",
            "Generate and copy the token",
        ],
        "env_var": "GITHUB_TOKEN",
        "test_endpoint": "https://api.github.com/rate_limit",
        "test_status": 200,
        "test_headers": {"Authorization": "Bearer {key}"},
        "category": "social",
        "used_by": ["scrapers.github_scraper"],
        "priority": "medium",
    },

    "telegram": {
        "name": "Telegram Bot API + MTProto",
        "provider": "Telegram",
        "signup_url": "https://my.telegram.org/apps",
        "free_tier": "Unlimited (MTProto), 30 msg/sec (Bot API)",
        "signup_method": "manual",
        "signup_steps": [
            "For Bot Token: message @BotFather on Telegram, send /newbot",
            "For API ID/Hash: go to https://my.telegram.org/apps",
            "Log in with your phone number",
            "Create a new application",
            "Note the api_id (number) and api_hash (string)",
        ],
        "env_var": "TELEGRAM_API_ID",
        "env_vars": {
            "TELEGRAM_API_ID": "",
            "TELEGRAM_API_HASH": "",
            "TELEGRAM_BOT_TOKEN": "",
        },
        "test_endpoint": "https://api.telegram.org/bot{key}/getMe",
        "test_status": 200,
        "category": "social",
        "used_by": ["collectors.telegram_channels", "monitoring.telegram_bot"],
        "priority": "high",
    },

    "discord": {
        "name": "Discord Bot API",
        "provider": "Discord / Snap",
        "signup_url": "https://discord.com/developers/applications",
        "free_tier": "50 requests/second",
        "signup_method": "manual",
        "signup_steps": [
            "Go to https://discord.com/developers/applications",
            "Click 'New Application'",
            "Go to Bot > Add Bot",
            "Copy the bot token",
            "Enable 'Message Content Intent' under Privileged Intents",
        ],
        "env_var": "DISCORD_BOT_TOKEN",
        "test_endpoint": "https://discord.com/api/v10/users/@me",
        "test_status": 200,
        "test_headers": {"Authorization": "Bot {key}"},
        "category": "social",
        "used_by": ["scrapers.discord_scraper"],
        "priority": "low",
    },

    # ── AI / LLM ───────────────────────────────────────────────
    "anthropic": {
        "name": "Anthropic Claude API",
        "provider": "Anthropic",
        "signup_url": "https://console.anthropic.com/settings/keys",
        "free_tier": "$5 free credits on signup",
        "signup_method": "manual",
        "signup_steps": [
            "Go to https://console.anthropic.com/",
            "Create an account",
            "Go to Settings > API Keys",
            "Create a new key",
        ],
        "env_var": "ANTHROPIC_API_KEY",
        "test_endpoint": None,
        "category": "ai",
        "used_by": ["processors.daily_digest", "api.routes.ask"],
        "priority": "medium",
    },

    "openweather": {
        "name": "OpenWeatherMap (Weather Data for Commodities)",
        "provider": "OpenWeather",
        "signup_url": "https://home.openweathermap.org/users/sign_up",
        "free_tier": "60 calls/min, current + 5-day forecast",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://home.openweathermap.org/users/sign_up",
            "Sign up with email",
            "API key is on the dashboard (may take ~2 hours to activate)",
        ],
        "env_var": "OPENWEATHER_API_KEY",
        "test_endpoint": "https://api.openweathermap.org/data/2.5/weather?q=Mumbai&appid={key}",
        "test_status": 200,
        "test_json_key": "main",
        "category": "alternative_data",
        "used_by": [],
        "priority": "low",
    },

    "nasdaq_data_link": {
        "name": "Nasdaq Data Link (Quandl)",
        "provider": "Nasdaq",
        "signup_url": "https://data.nasdaq.com/sign-up",
        "free_tier": "300 calls/10sec, 2000/10min, select datasets",
        "signup_method": "instant",
        "signup_steps": [
            "Go to https://data.nasdaq.com/sign-up",
            "Sign up with email",
            "API key is in Account Settings",
        ],
        "env_var": "NASDAQ_DATA_LINK_API_KEY",
        "test_endpoint": "https://data.nasdaq.com/api/v3/datasets/WIKI/AAPL.json?rows=1&api_key={key}",
        "test_status": 200,
        "category": "market_data",
        "used_by": [],
        "priority": "low",
    },
}


def get_by_category(category: str) -> dict[str, dict]:
    """Get all APIs in a category."""
    return {k: v for k, v in CATALOG.items() if v.get("category") == category}


def get_by_priority(priority: str) -> dict[str, dict]:
    """Get all APIs of a given priority."""
    return {k: v for k, v in CATALOG.items() if v.get("priority") == priority}


def get_all_env_vars() -> dict[str, str]:
    """Get all env vars from the catalog with their API names."""
    env_vars = {}
    for api_id, info in CATALOG.items():
        if info.get("env_vars"):
            for var in info["env_vars"]:
                env_vars[var] = api_id
        elif info.get("env_var"):
            env_vars[info["env_var"]] = api_id
    return env_vars


CATEGORIES = {
    "economic_data": "Central banks, government data, macro indicators",
    "market_data": "Stocks, bonds, forex, futures",
    "news": "News aggregators and search",
    "crypto": "Cryptocurrency market data",
    "forex": "Foreign exchange rates",
    "social": "Social media platforms",
    "ai": "AI/LLM providers",
    "alternative_data": "Weather, satellite, shipping, etc.",
}
