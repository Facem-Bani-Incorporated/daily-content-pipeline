import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List
from datetime import datetime, timedelta

# Importăm obiectul config din locația corectă (ajustează calea dacă e diferită)
from core.config import config
from core.logger import setup_logger

logger = setup_logger("Scraper")


class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}

        # Configurăm Cloudinary folosind obiectul Settings validat de Pydantic
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    async def fetch_today(self) -> List[dict]:
        """Extrage evenimentele zilei și pregătește slug-urile pentru procesare."""
        now = datetime.now()
        url = f"{config.WIKI_BASE_URL}/feed/onthisday/events/{now.month}/{now.day}"

        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()

                # Combinăm evenimentele principale cu cele secundare
                raw_events = data.get('selected', []) + data.get('events', [])

                processed_events = []
                for event in raw_events:
                    # Wikipedia API structure: extragem primul link valid
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
        """Calculăm vizualizările totale folosind slug-ul corect."""
        if not title_slug or title_slug == "history":
            return 0

        # Luăm datele pentru ultimele 30 de zile
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
        """Urcă imaginea pe Cloudinary și returnează noul URL securizat."""
        if not image_url:
            return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[{'width': 1000, 'crop': "limit", 'quality': "auto"}]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail ({public_id}): {e}")
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Extrage imagini din media-list-ul paginii de Wikipedia."""
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
                            img_src = item.get('srcset', [{}])[0].get('src') or item.get('title')
                            if img_src:
                                full_url = f"https:{img_src}" if img_src.startswith("//") else img_src
                                if ".svg" not in full_url.lower():
                                    image_urls.append(full_url)
                        if len(image_urls) >= limit:
                            break
            except Exception as e:
                logger.warning(f"⚠️ Gallery Fetch Fail for {title_slug}: {e}")

        # Fallback image dacă nu găsim nimic
        if not image_urls:
            image_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=800")
        return image_urls