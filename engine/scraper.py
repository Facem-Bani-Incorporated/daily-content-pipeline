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

    async def _get_image_metadata(self, url: str) -> Tuple[int, int]:
        """
        Interoghează API-ul Wikipedia pentru a afla rezoluția reală a fișierului.
        """
        if "upload.wikimedia.org" not in url:
            return (0, 0)
        
        file_name = url.split('/')[-1]
        if 'px-' in file_name: # Dacă e thumbnail, extragem numele original
             file_name = file_name.split('-', 1)[1]
        
        api_url = f"https://en.wikipedia.org/w/api.php?action=query&titles=File:{file_name}&prop=imageinfo&iiprop=size&format=json"
        
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=5.0) as client:
                res = await client.get(api_url)
                pages = res.json().get('query', {}).get('pages', {})
                for p in pages.values():
                    info = p.get('imageinfo', [{}])[0]
                    return info.get('width', 0), info.get('height', 0)
        except:
            return (0, 0)
        return (0, 0)

    async def _is_valid_high_res(self, url: str) -> Optional[str]:
        """
        Verifică dacă imaginea are peste 1000px și sub 10MB.
        Returnează URL-ul final (high-res) sau None.
        """
        if not url: return None
        
        # Transformăm în URL de rezoluție maximă
        high_res_url = url
        if "/thumb/" in url:
            parts = url.replace("/thumb/", "/").split("/")
            high_res_url = "/".join(parts[:-1]) if 'px-' in parts[-1] else "/".join(parts)

        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=5.0) as client:
                head = await client.head(high_res_url, follow_redirects=True)
                size = int(head.headers.get("Content-Length", 0))
                
                # Check 1: Mărime fișier (Limita Cloudinary 10MB)
                if size > 10400000: return None 
                
                # Check 2: Rezoluție (pixeli)
                w, h = await self._get_image_metadata(high_res_url)
                if w < 1000 and h < 1000: # Prea mică pentru "calitate înaltă"
                    return None
                    
                return high_res_url
        except:
            return None

    async def fetch_unsplash_fallback(self, query: str) -> Optional[str]:
        """Căutăm o poză artistică pe Unsplash dacă Wikipedia e slabă."""
        # Notă: Necesită un Access Key în config.UNSPLASH_ACCESS_KEY
        url = "https://api.unsplash.com/search/photos"
        params = {
            "query": query,
            "per_page": 1,
            "orientation": "landscape",
            "client_id": getattr(config, 'UNSPLASH_ACCESS_KEY', '')
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(url, params=params)
                if res.status_code == 200:
                    results = res.json().get('results', [])
                    if results: return results[0]['urls']['regular']
        except: pass
        return None

    def upload_to_cloudinary(self, image_url: str, public_id: str) -> Optional[str]:
        """Upload cu procesare AI pentru estetică maximă."""
        if not image_url: return None
        try:
            result = cloudinary.uploader.upload(
                image_url,
                public_id=f"history_app/{public_id}",
                overwrite=True,
                transformation=[
                    {'width': 1280, 'height': 720, 'crop': "fill"}, # Format cinematic
                    {'quality': "auto:best"},
                    {'fetch_format': "auto"},
                    {'gravity': "auto"} # AI Crop pe subiect
                ]
            )
            return result.get('secure_url')
        except Exception as e:
            logger.error(f"⚠️ Cloudinary Fail: {e}")
            return None

    async def fetch_gallery_urls(self, title_slug: str, limit: int = 3) -> List[str]:
        """Extrage doar poze 'reale' și mari."""
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
                            src = item.get('srcset', [{}])[-1].get('src') or item.get('title')
                            full_url = f"https:{src}" if src.startswith("//") else src
                            
                            # Filtru 1: Excludem gunoaiele vizuale
                            if any(bad in full_url.lower() for bad in [".svg", ".png", "icon", "logo", "map", "flag"]):
                                continue
                                
                            # Filtru 2: Check Rezoluție & Mărime
                            safe_url = await self._is_valid_high_res(full_url)
                            if safe_url:
                                valid_urls.append(safe_url)
                        
                        if len(valid_urls) >= limit: break
            except: pass

        # FALLBACK: Dacă n-avem poze bune pe Wiki, luăm una artistică de pe Unsplash
        if not valid_urls:
            unsplash_url = await self.fetch_unsplash_fallback(title_slug)
            if unsplash_url: valid_urls.append(unsplash_url)
            else: valid_urls.append("https://images.unsplash.com/photo-1447069387593-a5de0862481e?q=80&w=1280")
            
        return valid_urls

    async def fetch_page_views(self, title_slug: str) -> int:
        # Păstrat neschimbat (este corect în versiunea ta)
        if not title_slug or title_slug == "history": return 0
        yesterday = datetime.now() - timedelta(days=1)
        last_month = yesterday - timedelta(days=30)
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/{title_slug}/daily/{last_month.strftime('%Y%m%d')}/{yesterday.strftime('%Y%m%d')}"
        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    return sum(item['views'] for item in res.json().get('items', []))
            except: pass
        return 0