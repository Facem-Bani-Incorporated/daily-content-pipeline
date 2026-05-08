import asyncio
import time
import httpx
import cloudinary
import cloudinary.uploader
import cloudinary.exceptions
from typing import Optional, List
from core.config import config
from core.logger import setup_logger

logger = setup_logger("Scraper")


class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}
        self.pexels_key = getattr(config, "PEXELS_API_KEY", None)

        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    async def fetch_page_views(self, title_slug: str) -> int:
        if not title_slug:
            return 0

        from datetime import datetime, timedelta
        yesterday = datetime.now() - timedelta(days=1)
        start = (yesterday - timedelta(days=30)).strftime("%Y%m%d")
        end = yesterday.strftime("%Y%m%d")
        slug_clean = title_slug.replace(" ", "_")

        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/user/{slug_clean}/daily/{start}/{end}"
        )
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    return sum(item["views"] for item in res.json().get("items", []))
        except Exception:
            pass
        return 0

    async def fetch_pro_image(self, query: str) -> Optional[str]:
        if not self.pexels_key:
            return None

        url = f"https://api.pexels.com/v1/search?query={query}&per_page=1"
        headers = {"Authorization": self.pexels_key}
        try:
            async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    photos = res.json().get("photos", [])
                    if photos:
                        return photos[0]["src"]["large2x"]
        except Exception as e:
            logger.warning(f"Pexels fetch failed: {e}")
        return None

    async def _get_optimized_wiki_url(self, file_name: str) -> Optional[str]:
        file_clean = file_name.replace("File:", "").replace(" ", "_")
        api_url = (
            f"https://en.wikipedia.org/w/api.php"
            f"?action=query&titles=File:{file_clean}"
            f"&prop=imageinfo&iiprop=url|size&iiurlwidth=2000&format=json"
        )
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(api_url)
                pages = res.json().get("query", {}).get("pages", {})
                for p in pages.values():
                    info = p.get("imageinfo", [{}])[0]
                    return info.get("thumburl") or info.get("url")
        except Exception:
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        valid_urls = []
        slug_clean = title_slug.replace(" ", "_")
        wiki_media_url = f"{config.WIKI_BASE_URL}/page/media-list/{slug_clean}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(wiki_media_url)
                if res.status_code == 200:
                    items = res.json().get("items", [])
                    for item in items:
                        if len(valid_urls) >= limit:
                            break
                        title_lower = item.get("title", "").lower()
                        is_image = item.get("type") == "image"
                        is_clean = not any(
                            x in title_lower for x in [".svg", "map", "flag", "logo", "icon"]
                        )
                        if is_image and is_clean:
                            opt_url = await self._get_optimized_wiki_url(item.get("title"))
                            if opt_url and len(opt_url) <= 255:
                                valid_urls.append(opt_url)
                            elif opt_url:
                                logger.warning(f"Skipping URL too long ({len(opt_url)} chars): {opt_url[:80]}...")
            except Exception as e:
                logger.error(f"Gallery fetch error for '{title_slug}': {e}")

        return valid_urls

    def upload_to_cloudinary(
        self,
        image_url: str,
        public_id: str,
        max_retries: int = 3,
    ) -> Optional[str]:
        """
        Upload to Cloudinary with rate-limit handling.
        Optimizations:
          - SINGLE eager transformation (vs 4 chained before)
          - Exponential backoff on RateLimited
        """
        if not image_url:
            return None

        for attempt in range(1, max_retries + 1):
            try:
                result = cloudinary.uploader.upload(
                    image_url,
                    public_id=f"history_app/{public_id}",
                    overwrite=True,
                    eager=[
                        {
                            "width": 1600,
                            "height": 900,
                            "crop": "fill",
                            "gravity": "auto",
                            "quality": "auto:good",
                            "fetch_format": "auto",
                        }
                    ],
                    eager_async=False,
                )
                eager_urls = result.get("eager", [])
                if eager_urls:
                    return eager_urls[0].get("secure_url")
                return result.get("secure_url")

            except cloudinary.exceptions.RateLimited:
                wait = 2 ** attempt  # 2, 4, 8s
                logger.warning(
                    f"⏳ Cloudinary rate limited (attempt {attempt}/{max_retries}) "
                    f"for {public_id} — waiting {wait}s"
                )
                time.sleep(wait)

            except Exception as e:
                logger.error(f"⚠️ Cloudinary upload failed for {public_id}: {e}")
                return None

        logger.error(f"🚨 Cloudinary: gave up after {max_retries} retries for {public_id}")
        return None