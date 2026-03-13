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
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    async def _get_safe_high_res_url(self, url: str) -> str:
        """
        Verifică dacă imaginea originală (High Res) este sub limita de 10MB.
        Dacă e prea mare, returnează URL-ul de thumbnail care e deja optimizat.
        """
        if not url or "upload.wikimedia.org" not in url:
            return url
        
        # Generăm URL-ul pentru imaginea originală (full res)
        high_res_url = url
        if "/thumb/" in url:
            parts = url.replace("/thumb/", "/").split("/")
            if 'px-' in parts[-1]:
                high_res_url = "/".join(parts[:-1])
            else:
                high_res_url = "/".join(parts)

        # Verificăm mărimea fișierului fără a-l descărca complet (HTTP HEAD)
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=5.0) as client:
                head = await client.head(high_res_url, follow_redirects=True)
                size = int(head.headers.get("Content-Length", 0))
                
                # Limita Cloudinary Free este 10,485,760 bytes
                if size > 10000000: 
                    logger.warning(f"📏 Fișier prea mare ({size/1e6:.1f}MB). Folosesc varianta optimizată.")
                    return url # Returnăm URL-ul original de thumbnail (calitate medie)
                return high_res_url
        except Exception:
            return url

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

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Urcă imaginea pe Cloudinary cu procesare AI pentru încadrare."""
        if not image_url:
            return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {'width': 1200, 'crop': "limit"}, 
                    {'quality': "auto:good"},
                    {'fetch_format': "auto"},
                    {'gravity': "auto"} # AI detectează subiectul principal pentru crop
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail ({public_id}): {e}")
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Extrage imagini de calitate, evitând hărți/iconițe/SVG."""
        if not title_slug: return []
        
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
                            # Alegem cea mai mare variantă disponibilă din listă
                            best_src = srcset[-1].get('src') if srcset else item.get('title')
                            
                            if best_src:
                                full_url = f"https:{best_src}" if best_src.startswith("//") else best_src
                                # Filtru strict pentru calitate vizuală
                                if not any(bad in full_url.lower() for bad in [".svg", ".png", "icon", "logo", "map"]):
                                    # Verificăm mărimea și obținem URL-ul safe
                                    safe_url = await self._get_safe_high_res_url(full_url)
                                    image_urls.append(safe_url)
                        
                        if len(image_urls) >= limit: break
            except Exception as e:
                logger.warning(f"⚠️ Gallery Fetch Fail for {title_slug}: {e}")

        if not image_urls:
            image_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?q=80&w=1200")
            
        return image_urls

    async def fetch_page_views(self, title_slug: str) -> int:
        if not title_slug or title_slug == "history": return 0
        yesterday = datetime.now() - timedelta(days=1)
        last_month = yesterday - timedelta(days=30)
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{title_slug}/daily/{last_month.strftime('%Y%m%d')}/{yesterday.strftime('%Y%m%d')}"

        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    return sum(item['views'] for item in res.json().get('items', []))
            except Exception: pass
        return 0