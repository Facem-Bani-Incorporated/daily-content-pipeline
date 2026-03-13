import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List
from datetime import datetime, timedelta

# Importăm obiectul config din locația corectă
from core.config import config
from core.logger import setup_logger

logger = setup_logger("Scraper")

class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}

        # Configurăm Cloudinary
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    def _get_high_res_url(self, url: str) -> str:
        """
        Încearcă să obțină URL-ul imaginii originale eliminând segmentul de thumbnail.
        """
        if not url or "upload.wikimedia.org" not in url:
            return url
        
        if "/thumb/" in url:
            parts = url.replace("/thumb/", "/").split("/")
            # Dacă URL-ul se termină cu o dimensiune (ex: 200px-Nume.jpg), o eliminăm
            if 'px-' in parts[-1]:
                return "/".join(parts[:-1])
            return "/".join(parts)
        return url

    async def fetch_today(self) -> List[dict]:
        """Extrage evenimentele zilei de pe Wikipedia."""
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
        """Obține numărul de vizualizări pentru ultimele 30 de zile."""
        if not title_slug or title_slug == "history":
            return 0

        yesterday = datetime.now() - timedelta(days=1)
        last_month = yesterday - timedelta(days=30)
        start_str = last_month.strftime("%Y%m%d")
        end_str = yesterday.strftime("%Y%m%d")

        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{title_slug}/daily/{start_str}/{end_str}"

        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    return sum(item['views'] for item in data.get('items', []))
            except Exception as e:
                logger.warning(f"📊 PageViews Error for {title_slug}: {e}")
        return 0

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """
        Urcă imaginea pe Cloudinary. Încearcă High-Res, iar dacă e prea mare (>10MB),
        folosește automat thumbnail-ul original ca fallback.
        """
        if not image_url:
            return None
        
        # Surse de încercat: 1. Rezoluție Maximă, 2. Thumbnail standard (Fallback sigur)
        high_res = self._get_high_res_url(image_url)
        sources = [high_res, image_url]
        
        # Setăm transformările o singură dată
        upload_params = {
            "public_id": f"history_app/{public_id}",
            "overwrite": True,
            "transformation": [
                {'width': 1080, 'crop': "limit"},
                {'quality': "auto:best"},
                {'fetch_format': "auto"},
                {'gravity': "auto"}
            ]
        }

        for source in sources:
            if not source: continue
            try:
                result = cloudinary.uploader.upload(source, **upload_params)
                return result.get('secure_url')
            except Exception as e:
                error_msg = str(e)
                if "File size too large" in error_msg and source == high_res:
                    logger.warning(f"⚠️ Original too big for {public_id}, falling back to standard thumb...")
                    continue # Încearcă următoarea sursă din listă
                
                logger.error(f"⚠️ Cloudinary Fail ({public_id}): {error_msg}")
                break # Dacă e altă eroare sau a eșuat și fallback-ul, ne oprim
        
        return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Extrage imagini de calitate din media-list, filtrând elementele neadecvate."""
        if not title_slug:
            return []

        image_urls = []
        url = f"{config.WIKI_BASE_URL}/page/media-list/{title_slug.replace(' ', '_')}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    items = res.json().get('items', [])
                    for item in items:
                        if item.get('type') == 'image':
                            srcset = item.get('srcset', [])
                            img_src = srcset[-1].get('src') if srcset else item.get('title')

                            if img_src:
                                full_url = f"https:{img_src}" if img_src.startswith("//") else img_src
                                # Excludem formatele care nu sunt fotografii (hărți SVG, iconițe PNG)
                                if not any(bad in full_url.lower() for bad in [".svg", ".png", ".gif", "icon", "logo"]):
                                    image_urls.append(full_url)
                        
                        if len(image_urls) >= limit:
                            break
            except Exception as e:
                logger.warning(f"⚠️ Gallery Fetch Fail for {title_slug}: {e}")

        # Fallback dacă nu s-a găsit nicio imagine validă pe Wikipedia
        if not image_urls:
            image_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?q=80&w=1080")
            
        return image_urls