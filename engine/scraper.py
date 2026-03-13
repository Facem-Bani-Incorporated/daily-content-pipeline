import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List
from datetime import datetime, timedelta
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

    async def fetch_pro_image(self, query: str) -> Optional[str]:
        """Caută o poză HQ pe Pexels."""
        if not self.pexels_key:
            return None
            
        url = f"https://api.pexels.com/v1/search?query={query}&per_page=1"
        headers = {"Authorization": self.pexels_key}
        
        try:
            async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    photos = res.json().get('photos', [])
                    if photos:
                        return photos[0]['src']['large2x']
        except Exception as e:
            logger.warning(f"Pexels fetch failed: {e}")
        return None

    async def fetch_today(self) -> List[dict]:
        now = datetime.now()
        url = f"{config.WIKI_BASE_URL}/feed/onthisday/events/{now.month}/{now.day}"
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                raw_events = data.get('selected', []) + data.get('events', [])
                return [{
                    "year": e.get('year'),
                    "text": e.get('text'),
                    "slug": e.get('pages', [{}])[0].get('titles', {}).get('canonical') if e.get('pages') else None,
                    "wiki_thumb": e.get('pages', [{}])[0].get('thumbnail', {}).get('source') if e.get('pages') else None
                } for e in raw_events]
            except Exception as e:
                logger.error(f"❌ Wiki API Error: {e}")
                return []

    async def fetch_page_views(self, title_slug: str) -> int:
        if not title_slug: return 0
        yesterday = datetime.now() - timedelta(days=1)
        start = (yesterday - timedelta(days=30)).strftime('%Y%m%d')
        end = yesterday.strftime('%Y%m%d')
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{title_slug.replace(' ', '_')}/daily/{start}/{end}"
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    return sum(item['views'] for item in res.json().get('items', []))
        except: pass
        return 0

    async def _get_optimized_wiki_url(self, file_name: str) -> Optional[str]:
        file_clean = file_name.replace("File:", "").replace(" ", "_")
        api_url = f"https://en.wikipedia.org/w/api.php?action=query&titles=File:{file_clean}&prop=imageinfo&iiprop=url|size&iiurlwidth=2000&format=json"
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(api_url)
                pages = res.json().get('query', {}).get('pages', {})
                for p in pages.values():
                    info = p.get('imageinfo', [{}])[0]
                    return info.get('thumburl') or info.get('url')
        except: return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        valid_urls = []
        wiki_media_url = f"{config.WIKI_BASE_URL}/page/media-list/{title_slug.replace(' ', '_')}"
        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(wiki_media_url)
                if res.status_code == 200:
                    for item in res.json().get('items', []):
                        if item.get('type') == 'image' and not any(x in item.get('title','').lower() for x in [".svg", ".png", "map", "flag"]):
                            opt_url = await self._get_optimized_wiki_url(item.get('title'))
                            if opt_url: valid_urls.append(opt_url)
                        if len(valid_urls) >= limit: break
            except: pass
        return valid_urls

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        if not image_url: return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {'width': 1920, 'height': 1080, 'crop': "limit"},
                    {'width': 1600, 'height': 900, 'crop': "fill", 'gravity': "auto"},
                    {'quality': "auto:best"},
                    {'fetch_format': "auto"},
                    {'dpr': "auto"}
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail: {e}")
            return None