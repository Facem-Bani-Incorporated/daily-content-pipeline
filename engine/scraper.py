import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List
from core.config import config
from core.logger import setup_logger
from datetime import datetime, timedelta

logger = setup_logger("Scraper")

class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}
        # Configurare Cloudinary (asigură-te că ai config-ul încărcat)
        cloudinary.config(
            cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
            api_key=os.getenv("CLOUDINARY_API_KEY"),
            api_secret=os.getenv("CLOUDINARY_API_SECRET")
        )

    async def fetch_page_views(self, title_slug: str) -> int:
        if not title_slug or title_slug == "history": return 0

        # Luăm data de ieri pentru a asigura existența datelor
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

    async def fetch_today(self) -> List[dict]:
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
                    # Wikipedia pune link-urile în lista 'pages'
                    # Extragem primul slug valid pentru a avea source_url ulterior
                    pages = event.get('pages', [])
                    slug = pages[0].get('titles', {}).get('canonical') if pages else None
                    thumbnail = pages[0].get('thumbnail', {}).get('source') if pages else None

                    processed_events.append({
                        "year": event.get('year'),
                        "text": event.get('text'),
                        "slug": slug,
                        "wiki_thumb": thumbnail  # Îl păstrăm pentru upload ulterior
                    })
                return processed_events
            except Exception as e:
                logger.error(f"❌ Wiki API Error: {e}")
                return []

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 5) -> List[str]:
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
                            # Luăm cel mai bun URL disponibil
                            img_src = item.get('srcset', [{}])[0].get('src') or item.get('title')
                            if img_src:
                                full_url = f"https:{img_src}" if img_src.startswith("//") else img_src
                                if ".svg" not in full_url.lower():
                                    image_urls.append(full_url)
                        if len(image_urls) >= limit:
                            break
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch gallery for {title_slug}: {e}")

        # Fallback dacă Wiki nu are poze
        if not image_urls:
            image_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?w=800")
        return image_urls

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        # Nu mai verificăm "via.placeholder" aici pentru că acum trimitem link-uri reale de la Wikipedia
        if not image_url: return None
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