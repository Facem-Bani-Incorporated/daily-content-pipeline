import httpx
import cloudinary
import cloudinary.uploader
from typing import Optional, List, Tuple
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

    async def _get_optimized_wiki_url(self, file_name: str, preferred_width: int = 2000) -> Optional[str]:
        """
        În loc să luăm originalul de 50MB, cerem un thumbnail de rezoluție mare (ex: 2000px).
        Wiki va genera un JPG/WebP optimizat sub 10MB care arată impecabil pe orice ecran.
        """
        file_clean = file_name.replace("File:", "")
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
                    # thumburl este generat de Wiki la lățimea cerută (iiurlwidth)
                    return info.get('thumburl') or info.get('url')
        except Exception as e:
            logger.error(f"Error fetching high-res thumb: {e}")
        return None

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Urcă pe Cloudinary cu Smart Crop și compresie extremă."""
        if not image_url: return None
        try:
            # Sfat de CTO: Folosim 'fetch' dacă imaginea e deja online sau 'upload'
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                # Eager transformations: se fac la upload, nu la prima accesare
                transformation=[
                    # Crop inteligent bazat pe AI (g_auto) pentru a păstra fețele/subiectul
                    {'width': 1600, 'height': 900, 'crop': "fill", 'gravity': "auto"},
                    {'quality': "auto:good"}, # 'best' uneori face fișiere prea mari fără motiv
                    {'fetch_format': "auto"}  # Servește WebP/AVIF automat
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail: {e}")
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        if not title_slug: return []
        
        valid_urls = []
        # Media-list e bun, dar uneori returnează prea multe icoane/hărți
        wiki_media_url = f"{config.WIKI_BASE_URL}/page/media-list/{title_slug.replace(' ', '_')}"

        async with httpx.AsyncClient(headers=self.headers, timeout=15.0) as client:
            try:
                res = await client.get(wiki_media_url)
                if res.status_code == 200:
                    items = res.json().get('items', [])
                    for item in items:
                        if item.get('type') == 'image':
                            file_title = item.get('title')
                            
                            # 1. Filtrare agresivă "Anti-Gunoaie"
                            bad_keywords = [".svg", ".png", "icon", "logo", "map", "flag", "dispute", "edit-clear"]
                            if any(bad in file_title.lower() for bad in bad_keywords):
                                continue

                            # 2. Cerem versiunea de 2000px, nu originalul (pentru a fi sub 10MB)
                            optimized_url = await self._get_optimized_wiki_url(file_title)
                            
                            if optimized_url:
                                valid_urls.append(optimized_url)
                        
                        if len(valid_urls) >= limit: break
            except Exception as e:
                logger.error(f"Gallery fetch error: {e}")

        # Fallback de siguranță (Păstrat dar îmbunătățit)
        if not valid_urls:
            valid_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?q=80&w=1600")
            
        return valid_urls