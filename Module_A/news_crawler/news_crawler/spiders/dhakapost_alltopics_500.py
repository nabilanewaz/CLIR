import re
import scrapy
from scrapy.http import TextResponse
from urllib.parse import urlparse

ARTICLE_RE = re.compile(r"^https?://www\.dhakapost\.com/[^/]+/\d+/?$")


def source_to_category(source_url: str):
    """
    Converts list/source URL into a category label.
    Examples:
      https://www.dhakapost.com/sports -> category="sports"
      https://www.dhakapost.com/topic/xxxx -> category="topic", topic_slug="xxxx"
    """
    if not source_url:
        return ("unknown", None)

    if "/topic/" in source_url:
        topic_slug = source_url.rstrip("/").split("/topic/")[-1]
        return ("topic", topic_slug)

    path = urlparse(source_url).path.strip("/")  # e.g. "sports"
    if not path:
        return ("home", None)

    # take first path segment only
    category = path.split("/")[0]
    return (category, None)


def url_to_section(article_url: str):
    """
    Extracts article section from article URL.
    Example:
      https://www.dhakapost.com/jobs-career/425620 -> section="jobs-career"
    """
    try:
        path = urlparse(article_url).path.strip("/")
        parts = path.split("/")
        if len(parts) >= 2:
            return parts[0]
    except Exception:
        pass
    return None


class DhakaPostAllTopics500Spider(scrapy.Spider):
    name = "dhakapost_alltopics_500"
    allowed_domains = ["dhakapost.com"]

    custom_settings = {
        "CLOSESPIDER_ITEMCOUNT": 1500,   # stop after N items scraped
        "DOWNLOAD_DELAY": 0.3,
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "AUTOTHROTTLE_ENABLED": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    start_urls = ["https://www.dhakapost.com/"]

    max_clicks_per_source = 80
    target_unique_links = 2100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_links = set()

    async def start(self):
        """Scrapy 2.13+ start()"""
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_home,
                meta={"playwright": True, "playwright_include_page": True},
                dont_filter=True,
            )

    async def parse_home(self, response):
        page = response.meta["playwright_page"]
        await page.wait_for_timeout(1200)

        # Main sections (stable)
        sections = [
            "https://www.dhakapost.com/latest-news",
            "https://www.dhakapost.com/popular-bangla-news",
            "https://www.dhakapost.com/national",
            "https://www.dhakapost.com/politics",
            "https://www.dhakapost.com/economy",
            "https://www.dhakapost.com/international",
            "https://www.dhakapost.com/country",
            "https://www.dhakapost.com/sports",
            "https://www.dhakapost.com/entertainment",
        ]

        # Grab topic links automatically from homepage
        topic_urls = await page.eval_on_selector_all(
            "a[href^='https://www.dhakapost.com/topic/']",
            "els => Array.from(new Set(els.map(e => e.href)))",
        )

        sources = list(dict.fromkeys(sections + topic_urls))
        self.logger.info("Collected %d sources (sections + topics).", len(sources))

        await page.close()

        for src in sources:
            category, topic_slug = source_to_category(src)

            yield scrapy.Request(
                src,
                callback=self.parse_list,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "source": src,
                    "category": category,
                    "topic_slug": topic_slug,
                },
                dont_filter=True,
            )

    async def parse_list(self, response):
        source = response.meta.get("source", response.url)
        category = response.meta.get("category", "unknown")
        topic_slug = response.meta.get("topic_slug")

        page = response.meta["playwright_page"]
        await page.wait_for_timeout(900)

        last_total = 0
        stale_rounds = 0

        for click_i in range(self.max_clicks_per_source):
            if len(self.seen_links) >= self.target_unique_links:
                break

            hrefs = await page.eval_on_selector_all(
                "a.group[href]",
                "els => els.map(e => e.href)",
            )

            new_count = 0
            for url in hrefs:
                if not ARTICLE_RE.match(url):
                    continue
                if url in self.seen_links:
                    continue

                self.seen_links.add(url)
                new_count += 1

                yield scrapy.Request(
                    url,
                    callback=self.parse_article,
                    meta={
                        "source_list": source,
                        "category": category,
                        "topic_slug": topic_slug,
                    },
                    dont_filter=True,
                )

            self.logger.info(
                "LIST %s click=%d -> links=%d | NEW=%d | unique_total=%d | category=%s",
                response.url,
                click_i + 1,
                len(hrefs),
                new_count,
                len(self.seen_links),
                category,
            )

            # Stop if no new links repeatedly
            if len(self.seen_links) == last_total:
                stale_rounds += 1
            else:
                stale_rounds = 0
                last_total = len(self.seen_links)

            if stale_rounds >= 5:
                self.logger.info("Stopping %s: no new links after 5 clicks.", source)
                break

            # Click "আরও দেখুন"
            btn = page.get_by_role("button", name="আরও দেখুন")
            if await btn.count() == 0:
                btn = page.locator("text=আরও দেখুন")

            if await btn.count() == 0:
                break

            try:
                await btn.first.click(timeout=5000)
                await page.wait_for_timeout(1300)
            except Exception:
                break

        await page.close()

    def parse_article(self, response):
        # Safety: skip if response is not text/html
        if not isinstance(response, TextResponse):
            self.logger.warning("Skipping non-text response: %s", response.url)
            return

        title = response.css("h1::text").get() or response.xpath("//h1/text()").get()
        title = title.strip() if title else None

        author = (
            response.xpath(
                "//*[contains(@class,'author') or contains(@class,'writer')]/text()"
            ).get()
            or response.xpath("//p[contains(@class,'author')]/text()").get()
        )
        author = author.strip() if author else None

        date = response.css("time::text").get()
        if date:
            date = date.strip()

        paras = response.xpath("//article//p//text() | //main//p//text()").getall()
        cleaned = [t.strip().replace("\xa0", " ") for t in paras if t.strip()]

        bad = {"আরও পড়ুন", "ফলো করুন", "বিজ্ঞাপন", "লোড হচ্ছে ..."}
        cleaned = [t for t in cleaned if t not in bad]

        body = " ".join(cleaned)

        # category from source_list (sports/politics/etc)
        category = response.meta.get("category", "unknown")
        topic_slug = response.meta.get("topic_slug")

        # section from article URL itself (sometimes better)
        section = url_to_section(response.url)

        yield {
            "title": title,
            "body": body,
            "url": response.url,
            "date": date,
            "language": "bn",
            "author": author,
            "tokens": len(body.split()),
            "section": section,        # extracted from article URL  
        }
