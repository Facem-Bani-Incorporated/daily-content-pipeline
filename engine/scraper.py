import httpx
import cloudinary
import cloudinary.uploader
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
        """Fetch 30-day Wikipedia page views for a given article slug."""
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
        """Fetch a high-quality image from Pexels for the given query."""
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
        """Resolve a Wikipedia File: title to a 2000px-wide optimized image URL."""
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
        """
        Fetch up to `limit` clean image URLs from a Wikipedia article.
        The slug comes directly from the AI — no feed dependency.
        Filters out SVGs, maps, flags, and GIFs.
        """
        valid_urls = []
        slug_clean = title_slug.replace(" ", "_")
        wiki_media_url = f"{config.WIKI_BASE_URL}/page/media-list/{slug_clean}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(wiki_media_url)
                if res.status_code == 200:
                    items = res.json().get("items", [])
                    for item in items:
                        title_lower = item.get("title", "").lower()
                        is_image = item.get("type") == "image"
                        is_clean = not any(
                            x in title_lower for x in [".svg", "map", "flag", "logo", "icon"]
                        )
                        if is_image and is_clean:
                            opt_url = await self._get_optimized_wiki_url(item.get("title"))
                            if opt_url:
                                valid_urls.append(opt_url)
                        if len(valid_urls) >= limit:
                            break
            except Exception as e:
                logger.error(f"Gallery fetch error for '{title_slug}': {e}")

        return valid_urls

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Upload an image URL to Cloudinary with quality/size optimization."""
        if not image_url:
            return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {"width": 1920, "height": 1080, "crop": "limit"},
                    {"width": 1600, "height": 900, "crop": "fill", "gravity": "auto"},
                    {"quality": "auto:best"},
                    {"fetch_format": "auto"},
                    {"dpr": "auto"},
                ],
            )
            return result.get("secure_url")
        except Exception as e:
            logger.error(f"⚠️ Cloudinary upload failed: {e}")
            return None