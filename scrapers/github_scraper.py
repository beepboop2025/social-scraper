"""GitHub scraper — issues, discussions, releases, and trending repos."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from models import (
    AuthorInfo, ContentType, EngagementMetrics, Platform,
    ScrapedContent, ScrapedItem,
)
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Financial/crypto repos to monitor
DEFAULT_REPOS = [
    "bitcoin/bitcoin",
    "ethereum/go-ethereum",
    "solana-labs/solana",
    "aave/aave-v3-core",
    "Uniswap/v3-core",
    "MakerDAO/dss",
    "compound-finance/compound-protocol",
    "OpenZeppelin/openzeppelin-contracts",
    "freqtrade/freqtrade",
    "ccxt/ccxt",
    "QuantConnect/Lean",
    "zipline-live/zipline",
    "ranaroussi/yfinance",
    "microsoft/qlib",
]


class GitHubScraper(BaseScraper):
    """Scrape GitHub issues, discussions, releases, and trending repos.

    Uses the GitHub REST API. Optional personal access token for higher rate limits.
    """

    platform = Platform.GITHUB
    name = "github"

    BASE_URL = "https://api.github.com"

    def __init__(self, token: Optional[str] = None, **kwargs):
        super().__init__(rate_limit=25, **kwargs)
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "SocialScraper/3.0",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.AsyncClient(timeout=30, headers=headers)

    async def close(self):
        """Close the HTTP client."""
        await self._http.aclose()

    async def _get(self, endpoint: str, params: Optional[dict] = None) -> dict | list:
        resp = await self._http.get(f"{self.BASE_URL}{endpoint}", params=params)
        resp.raise_for_status()
        return resp.json()

    def _parse_issue(self, issue: dict, repo: str) -> ScrapedItem:
        user = issue.get("user", {})
        labels = [l.get("name", "") for l in issue.get("labels", [])]
        is_pr = "pull_request" in issue

        content = ScrapedContent(
            id=self.make_id("github", repo, str(issue.get("number", ""))),
            platform=Platform.GITHUB,
            content_type=ContentType.ISSUE,
            text=f"[{'PR' if is_pr else 'Issue'}] {issue.get('title', '')}\n\n{issue.get('body', '') or ''}",
            author=AuthorInfo(
                username=user.get("login", ""),
                display_name=user.get("login", ""),
                id=str(user.get("id", "")),
                avatar_url=user.get("avatar_url"),
            ),
            engagement=EngagementMetrics(
                likes=issue.get("reactions", {}).get("+1", 0) + issue.get("reactions", {}).get("heart", 0),
                replies=issue.get("comments", 0),
            ),
            created_at=datetime.fromisoformat(issue["created_at"].replace("Z", "+00:00"))
            if issue.get("created_at") else datetime.now(timezone.utc),
            source_url=issue.get("html_url", ""),
            source_channel=repo,
            hashtags=labels,
            raw_metadata={
                "repo": repo,
                "number": issue.get("number"),
                "state": issue.get("state"),
                "is_pull_request": is_pr,
                "labels": labels,
                "milestone": issue.get("milestone", {}).get("title") if issue.get("milestone") else None,
                "assignees": [a.get("login") for a in issue.get("assignees", [])],
            },
            tags=labels,
        )
        return ScrapedItem(unified=content)

    def _parse_release(self, release: dict, repo: str) -> ScrapedItem:
        author = release.get("author", {})

        content = ScrapedContent(
            id=self.make_id("github", "release", repo, release.get("tag_name", "")),
            platform=Platform.GITHUB,
            content_type=ContentType.RELEASE,
            text=f"[Release {release.get('tag_name', '')}] {release.get('name', '')}\n\n{release.get('body', '') or ''}",
            author=AuthorInfo(
                username=author.get("login", ""),
                display_name=author.get("login", ""),
                avatar_url=author.get("avatar_url"),
            ),
            engagement=EngagementMetrics(),
            created_at=datetime.fromisoformat(release["published_at"].replace("Z", "+00:00"))
            if release.get("published_at") else datetime.now(timezone.utc),
            source_url=release.get("html_url", ""),
            source_channel=repo,
            raw_metadata={
                "repo": repo,
                "tag_name": release.get("tag_name"),
                "prerelease": release.get("prerelease", False),
                "draft": release.get("draft", False),
                "assets_count": len(release.get("assets", [])),
            },
            tags=["release", release.get("tag_name", "")],
        )
        return ScrapedItem(unified=content)

    async def scrape(self, query: str, limit: int = 50) -> list[ScrapedItem]:
        """Search GitHub issues/PRs across all repos."""
        data = await self._get("/search/issues", {
            "q": f"{query} is:open",
            "sort": "created",
            "order": "desc",
            "per_page": min(limit, 100),
        })
        return [self._parse_issue(i, i.get("repository_url", "").split("/repos/")[-1]) for i in data.get("items", [])]

    async def scrape_channel(self, channel_id: str, limit: int = 50) -> list[ScrapedItem]:
        """Scrape recent issues from a repo."""
        issues = await self._get(f"/repos/{channel_id}/issues", {
            "state": "all",
            "sort": "created",
            "direction": "desc",
            "per_page": min(limit, 100),
        })
        return [self._parse_issue(i, channel_id) for i in issues]

    async def scrape_releases(self, repo: str, limit: int = 10) -> list[ScrapedItem]:
        """Scrape releases from a repo."""
        releases = await self._get(f"/repos/{repo}/releases", {"per_page": min(limit, 30)})
        return [self._parse_release(r, repo) for r in releases]

    async def scrape_trending(self) -> list[ScrapedItem]:
        """Scrape trending repos (using search as proxy)."""
        data = await self._get("/search/repositories", {
            "q": "topic:cryptocurrency OR topic:trading OR topic:fintech created:>2026-03-01",
            "sort": "stars",
            "order": "desc",
            "per_page": 30,
        })

        items = []
        for repo in data.get("items", []):
            owner = repo.get("owner", {})
            content = ScrapedContent(
                id=self.make_id("github", "repo", repo.get("full_name", "")),
                platform=Platform.GITHUB,
                content_type=ContentType.POST,
                text=f"[Trending Repo] {repo.get('full_name', '')}\n\n{repo.get('description', '') or ''}",
                author=AuthorInfo(
                    username=owner.get("login", ""),
                    display_name=owner.get("login", ""),
                    avatar_url=owner.get("avatar_url"),
                ),
                engagement=EngagementMetrics(
                    likes=repo.get("stargazers_count", 0),
                    reposts=repo.get("forks_count", 0),
                    views=repo.get("watchers_count", 0),
                ),
                created_at=datetime.fromisoformat(repo["created_at"].replace("Z", "+00:00"))
                if repo.get("created_at") else datetime.now(timezone.utc),
                source_url=repo.get("html_url", ""),
                source_channel=repo.get("full_name", ""),
                hashtags=repo.get("topics", []),
                raw_metadata={
                    "full_name": repo.get("full_name"),
                    "language": repo.get("language"),
                    "stars": repo.get("stargazers_count"),
                    "forks": repo.get("forks_count"),
                    "open_issues": repo.get("open_issues_count"),
                    "license": repo.get("license", {}).get("spdx_id") if repo.get("license") else None,
                },
                tags=repo.get("topics", []),
            )
            items.append(ScrapedItem(unified=content))
        return items

    async def scrape_all_monitored(self, limit_per_repo: int = 10) -> list[ScrapedItem]:
        """Scrape issues + releases from all monitored repos."""
        all_items = []
        for repo in DEFAULT_REPOS:
            issues = await self.safe_scrape_channel(repo, limit_per_repo)
            all_items.extend(issues)
            releases = await self.scrape_releases(repo, 3)
            all_items.extend(releases)
            await asyncio.sleep(0.5)

        trending = await self.scrape_trending()
        all_items.extend(trending)

        logger.info(f"[GitHub] Scraped {len(all_items)} items from {len(DEFAULT_REPOS)} repos + trending")
        return all_items
