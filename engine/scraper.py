import httpx
import asyncio
import cloudinary
import cloudinary.uploader
from typing import Optional, List, Dict
from core.config import config
from core.logger import setup_logger

logger = setup_logger("WikiScraper")

class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": f"HistoryApp/1.0 ({config.CONTACT_EMAIL})"}
        cloudinary.config(
            cloud_name=config.CLOUDINARY_CLOUD_NAME,
            api_key=config.CLOUDINARY_API_KEY,
            api_secret=config.CLOUDINARY_API_SECRET
        )

    async def fetch_on_this_day_raw(self, month: int, day: int) -> List[Dict]:
        """
        Sursa de Adevar: Extrage toate evenimentele verificate de Wikipedia pentru data respectiva.
        Elimina riscul de halucinatie a datei.
        """
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/{month:02d}/{day:02d}"
        
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
                res = await client.get(url)
                if res.status_code != 200:
                    logger.error(f"Wiki API Error: {res.status_code}")
                    return []
                
                data = res.json()
                # Combinam evenimentele 'selected' (curate de editori) cu cele standard
                raw_events = data.get("selected", []) + data.get("events", [])
                
                processed = []
                for ev in raw_events:
                    pages = ev.get("pages", [])
                    if not pages: continue
                    
                    # Extragem doar datele esentiale pentru filtrare
                    processed.append({
                        "year": ev.get("year"),
                        "text": ev.get("text"),
                        "slug": pages[0].get("titles", {}).get("canonical"),
                        "wiki_thumb": pages[0].get("thumbnail", {}).get("source") if pages[0].get("thumbnail") else None,
                        "description": pages[0].get("description", "")
                    })
                return processed
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            return []

    async def get_popularity_score(self, slug: str) -> int:
        """
        Filtru de Relevanta: Cate vizualizari a avut subiectul in ultimele 30 de zile.
        Daca slug-ul e 'French_Revolution', va avea scor mare. Daca e un nobil uitat, scor mic.
        """
        from datetime import datetime, timedelta
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        slug_clean = slug.replace(" ", "_")
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/user/{slug_clean}/daily/{start}/{end}"
        )
        
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=5.0) as client:
                res = await client.get(url)
                if res.status_code == 200:
                    items = res.json().get("items", [])
                    return sum(item["views"] for item in items)
        except:
            pass
        return 0

    async def get_optimized_image(self, slug: str) -> Optional[str]:
        """Gaseste imaginea de rezolutie mare (2000px) pentru fundalul aplicatiei."""
        api_url = (
            f"https://en.wikipedia.org/w/api.php?action=query&titles={slug}"
            f"&prop=pageimages|imageinfo&iiprop=url&pithumbsize=2000&format=json"
        )
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
                res = await client.get(api_url)
                pages = res.json().get("query", {}).get("pages", {})
                for p in pages.values():
                    return p.get("thumbnail", {}).get("source")
        except:
            return None
        return None

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Salvare permanenta in Cloudinary cu optimizare automata pentru Mobile."""
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {"width": 1080, "height": 1920, "crop": "fill", "gravity": "auto"},
                    {"quality": "auto:good"},
                    {"fetch_format": "auto"}
                ]
            )
            return result.get("secure_url")
        except Exception as e:
            logger.error(f"Cloudinary error: {e}")
            return None