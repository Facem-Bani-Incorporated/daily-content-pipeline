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

        # Configurăm Cloudinary folosind obiectul Settings validat de Pydantic
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    def _get_high_res_url(self, url: str) -> str:
        """
        Transformă un URL de thumbnail Wikipedia în URL-ul imaginii originale.
        Exemplu: dintr-un thumb de 200px extrage sursa originală de câțiva MB.
        """
        if not url or "upload.wikimedia.org" not in url:
            return url
        
        if "/thumb/" in url:
            # Structura URL-ului de thumb la Wiki: .../thumb/a/a1/Nume.jpg/200px-Nume.jpg
            # Eliminăm '/thumb/' și ultima parte (dimensiunea) pentru a obține originalul.
            parts = url.replace("/thumb/", "/").split("/")
            # Verificăm dacă ultima parte este o redimensionare (ex: '200px-...')
            if parts[-1].lower().endswith(('.jpg', '.jpeg', '.png', '.webp')) and 'px-' in parts[-1]:
                return "/".join(parts[:-1])
            return "/".join(parts)
        return url

    async def fetch_today(self) -> List[dict]:
        """Extrage evenimentele zilei și pregătește slug-urile pentru procesare."""
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
        """Calculăm vizualizările totale folosind slug-ul corect."""
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
        """Urcă imaginea la rezoluție maximă pe Cloudinary cu optimizări vizuale."""
        if not image_url:
            return None
        try:
            # Convertim thumb-ul în High Res înainte de upload
            target_url = self._get_high_res_url(image_url)
            
            result = cloudinary.uploader.upload(
                target_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {'width': 1080, 'crop': "limit"}, # Rezoluție bună pentru mobil
                    {'quality': "auto:best"},        # Compresie inteligentă
                    {'fetch_format': "auto"},        # Servește WebP/AVIF automat
                    {'gravity': "auto"}              # Crop inteligent pe subiect (AI)
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail ({public_id}): {e}")
            # Fallback la URL-ul original dacă procesarea high-res eșuează
            return image_url if image_url.startswith("http") else None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Extrage imagini de calitate din galeria paginii."""
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
                            # Încercăm să luăm cea mai mare rezoluție din srcset
                            srcset = item.get('srcset', [])
                            if srcset:
                                # Luăm ultimul element din srcset (cel mai mare de obicei)
                                img_src = srcset[-1].get('src')
                            else:
                                img_src = item.get('title')

                            if img_src:
                                full_url = f"https:{img_src}" if img_src.startswith("//") else img_src
                                # Filtru: Fără SVG-uri, iconițe sau fișiere ciudate
                                if not any(bad in full_url.lower() for bad in [".svg", ".png", ".gif", "icon"]):
                                    image_urls.append(full_url)
                        
                        if len(image_urls) >= limit:
                            break
            except Exception as e:
                logger.warning(f"⚠️ Gallery Fetch Fail for {title_slug}: {e}")

        # Fallback dacă Wikipedia nu are poze relevante: Unsplash High-Res
        if not image_urls:
            # Folosim un keyword compus pentru relevanță
            search_term = title_slug.replace('_', ',')
            image_urls.append(f"https://images.unsplash.com/photo-1599827553299-a8a7600724d4?q=80&w=1080") # General History Fallback
            
        return image_urls