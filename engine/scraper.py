import httpx
from datetime import datetime
from typing import List, Dict
import asyncio

from core.config import config
from core.logger import setup_logger

logger = setup_logger("EliteScraper")

class SmartWikiScraper:
    def __init__(self):
        self.headers = {"User-Agent": f"HistoryApp/1.0 ({config.CONTACT_EMAIL})"}
        # Cuvinte care semnalează un articol plictisitor/administrativ
        self.blacklist_terms = [
            "country", "republic", "state", "continent", "capital city", 
            "administrative", "municipality", "census-designated", "village"
        ]

    async def fetch_page_views(self, slug: str) -> int:
        """Calculăm popularitatea reală pe ultimele 30 de zile."""
        from datetime import timedelta
        today_str = datetime.now().strftime("%Y%m%d")
        last_month_str = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
               f"en.wikipedia/all-access/user/{slug}/daily/{last_month_str}/{today_str}")
        
        async with httpx.AsyncClient(headers=self.headers, timeout=5.0) as client:
            try:
                res = await client.get(url)
                if res.status_code == 200:
                    data = res.json()
                    return sum(item["views"] for item in data.get("items", []))
            except:
                return 0
        return 0

    async def get_today_elite_events(self) -> List[Dict]:
        """
        Punctul cheie: Folosim endpoint-ul 'selected' care conține DOAR 
        evenimente verificate manual de editorii Wikipedia ca fiind importante.
        """
        now = datetime.now()
        # Endpoint-ul 'all' ne dă 'selected', 'events', 'births', 'deaths'
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/{now.month:02d}/{now.day:02d}"

        async with httpx.AsyncClient(headers=self.headers, timeout=20.0) as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                data = res.json()

                # PRIORITATE 1: Evenimentele 'selected' (curate de oameni)
                # PRIORITATE 2: Evenimentele 'events' (brute, dacă 'selected' e prea scurt)
                raw_list = data.get("selected", [])
                if len(raw_list) < 10:
                    raw_list += data.get("events", [])

                clean_events = []
                for ev in raw_list:
                    pages = ev.get("pages", [])
                    if not pages: continue
                    
                    main_page = pages[0]
                    description = main_page.get("description", "").lower()
                    
                    # FILTRU DE ELITĂ: Eliminăm "gunoiul" administrativ
                    if any(term in description for term in self.blacklist_terms):
                        continue

                    # Eliminăm evenimentele fără an sau text
                    if not ev.get("year") or not ev.get("text"):
                        continue

                    clean_events.append({
                        "year": ev["year"],
                        "text": ev["text"],
                        "slug": main_page.get("titles", {}).get("canonical"),
                        "description": description,
                        "thumbnail": main_page.get("thumbnail", {}).get("source") if main_page.get("thumbnail") else None
                    })

                return clean_events

            except Exception as e:
                logger.error(f"❌ Scraper failed: {e}")
                return []