import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List, Tuple
from datetime import datetime, timedelta

from core.config import config
from core.logger import setup_logger

logger = setup_logger("Scraper")

class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    async def fetch_today(self) -> List[dict]:
        """Extrage evenimentele brute de pe Wikipedia Feed API."""
        now = datetime.now()
        url = f"{config.WIKI_BASE_URL}/feed/onthisday/events/{now.month}/{now.day}"

        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                raw_events = data.get('selected', []) + data.get('events', [])

                processed_events = []
                for event in raw_events:
                    pages = event.get('pages', [])
                    slug = pages[0].get('titles', {}).get('canonical') if pages else None
                    thumbnail = pages[0].get('thumbnail', {}).get('source') if pages else None

                    processed_events.append({
                        "year": event.get('year'),
                        "text": event.get('text'),
                        "slug": slug,
                        "wiki_thumb": thumbnail
                    })
                return processed_events
            except Exception as e:
                logger.error(f"❌ Wiki API Error: {e}")
                return []

    async def fetch_page_views(self, title_slug: str) -> int:
        """
        Calculează popularitatea unui eveniment. 
        Esențial pentru algoritmul de Impact Score.
        """
        if not title_slug or title_slug == "history": 
            return 0
            
        yesterday = datetime.now() - timedelta(days=1)
        last_month = yesterday - timedelta(days=30)
        
        # API-ul de metrics cere format YYYYMMDD
        start_date = last_month.strftime('%Y%m%d')
        end_date = yesterday.strftime('%Y%m%d')
        
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/user/{title_slug.replace(' ', '_')}/daily/{start_date}/{end_date}"
        )
        
        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    items = res.json().get('items', [])
                    return sum(item['views'] for item in items)
            except Exception as e:
                logger.warning(f"Could not fetch views for {title_slug}: {e}")
        return 0

    async def _get_optimized_wiki_url(self, file_name: str, preferred_width: int = 2000) -> Optional[str]:
        """Conversie automată via MediaWiki pentru a rămâne sub 10MB."""
        file_clean = file_name.replace("File:", "").replace(" ", "_")
        api_url = (
            "https://en.wikipedia.org/w/api.php?action=query"
            f"&titles=File:{file_clean}&prop=imageinfo"
            f"&iiprop=url|size&iiurlwidth={preferred_width}&format=json"
        )
        
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(api_url)
                pages = res.json().get('query', {}).get('pages', {})
                for p in pages.values():
                    info = p.get('imageinfo', [{}])[0]
                    return info.get('thumburl') or info.get('url')
        except Exception:
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Obține URL-uri de imagini de calitate, gata de procesat."""
        if not title_slug: return []
        
        valid_urls = []
        wiki_media_url = f"{config.WIKI_BASE_URL}/page/media-list/{title_slug.replace(' ', '_')}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(wiki_media_url)
                if res.status_code == 200:
                    items = res.json().get('items', [])
                    for item in items:
                        if item.get('type') == 'image':
                            file_title = item.get('title')
                            
                            # Filtrare: Fără diagrame sau elemente de UI
                            bad_keywords = [".svg", ".png", "icon", "logo", "map", "flag", "dispute", "chart"]
                            if any(bad in file_title.lower() for bad in bad_keywords):
                                continue

                            optimized_url = await self._get_optimized_wiki_url(file_title)
                            if optimized_url:
                                valid_urls.append(optimized_url)
                        
                        if len(valid_urls) >= limit: break
            except Exception:
                pass

        if not valid_urls:
            valid_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?q=80&w=1600")
            
        return valid_urls

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Finalizează procesul cu Smart Crop și stocare Cloud."""
        if not image_url: return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {'width': 1600, 'height': 900, 'crop': "fill", 'gravity': "auto"},
                    {'quality': "auto:good"},
                    {'fetch_format': "auto"}
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail: {e}")
            return None