import httpx
from typing import List
from core.config import config


class WikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": config.USER_AGENT}

    # ─────────────────────────────────────────
    # PAGE VIEWS
    # ─────────────────────────────────────────
    async def fetch_page_views(self, slug: str) -> int:
        if not slug:
            return 0

        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/{slug}/daily/20240101/20240201"

        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                data = res.json()
                return sum(item["views"] for item in data.get("items", []))
            except Exception:
                return 0

    # ─────────────────────────────────────────
    # PRO IMAGE (PEXELS FAKE SAFE)
    # ─────────────────────────────────────────
    async def fetch_pro_image(self, query: str) -> str | None:
        return f"https://source.unsplash.com/featured/?{query.replace(' ', ',')}"

    # ─────────────────────────────────────────
    # WIKI GALLERY
    # ─────────────────────────────────────────
    async def fetch_gallery_urls(self, slug: str, limit: int = 3) -> List[str]:
        url = f"https://en.wikipedia.org/api/rest_v1/page/media-list/{slug}"

        async with httpx.AsyncClient(headers=self.headers, timeout=10.0) as client:
            try:
                res = await client.get(url)
                data = res.json()

                images = []
                for item in data.get("items", []):
                    src = item.get("srcset", [])
                    if src:
                        images.append(src[-1]["src"])
                    if len(images) >= limit:
                        break

                return images
            except Exception:
                return []

    # ─────────────────────────────────────────
    # CLOUDINARY MOCK (SAFE)
    # ─────────────────────────────────────────
    def upload_to_cloudinary(self, url: str, public_id: str) -> str:
        return url  # 🔥 fallback simplu (merge sigur)