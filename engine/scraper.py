import httpx
from datetime import datetime, timedelta
from typing import List, Dict
import asyncio
from core.logger import setup_logger

logger = setup_logger("EliteScraper")

class SmartWikiScraper:
    def __init__(self):
        # Un User-Agent generic, curat, care respectă politicile Wiki, fără variabile externe
        self.headers = {"User-Agent": "EliteHistoryBot/1.0 (Educational API)"}
        
        # O listă neagră agresivă pentru a elimina gunoiul (nașteri obscure, orașe, etc.)
        self.blacklist_terms = [
            "country", "republic", "state", "continent", "capital city", 
            "administrative", "municipality", "census-designated", "village",
            "province", "territory", "region", "district", "footballer", 
            "cricketer", "baseball player", "politician from"
        ]

    async def fetch_page_views(self, slug: str) -> int:
        """Trage vizualizările reale pe ultimele 30 de zile pentru a calcula popularitatea."""
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
            except Exception:
                return 0
        return 0

    async def get_today_elite_events(self) -> List[Dict]:
        """Extrage DOAR evenimentele istorice premium pentru exact ziua de azi."""
        now = datetime.now()
        logger.info(f"🗓️ Căutăm evenimente de top pentru: {now.day:02d}/{now.month:02d}")
        
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/{now.month:02d}/{now.day:02d}"

        async with httpx.AsyncClient(headers=self.headers, timeout=20.0) as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                data = res.json()

                # Prioritate MAXIMĂ: secțiunea "selected" (curatoriată de oameni)
                raw_list = data.get("selected", [])
                
                # Dacă sunt prea puține, completăm cu evenimente standard, dar le vom penaliza la scor
                if len(raw_list) < 10:
                    raw_list.extend(data.get("events", [])[:15])

                clean_events = []
                seen_texts = set()
                
                for ev in raw_list:
                    year = ev.get("year")
                    text = ev.get("text", "").strip()
                    pages = ev.get("pages", [])
                    
                    if not year or not text or not pages or text in seen_texts:
                        continue
                        
                    seen_texts.add(text)
                    main_page = pages[0]
                    slug = main_page.get("titles", {}).get("canonical", "")
                    description = main_page.get("description", "").lower()
                    extract = main_page.get("extract", "")
                    
                    # Filtru de Elită: Eliminăm non-subiecte și articole prea scurte
                    if not slug or len(extract) < 80:
                        continue
                    if any(term in description for term in self.blacklist_terms):
                        continue
                    
                    clean_events.append({
                        "year": int(year),
                        "text": text,
                        "slug": slug,
                        "description": description,
                        "extract": extract[:600],
                        "thumbnail": main_page.get("thumbnail", {}).get("source") if main_page.get("thumbnail") else None,
                        "month": now.month,
                        "day": now.day,
                        "is_selected": ev in data.get("selected", []) # Flag pentru bonus la scor
                    })
                
                logger.info(f"✅ Găsite {len(clean_events)} evenimente valide.")
                return clean_events

            except Exception as e:
                logger.error(f"❌ Scraperul a picat: {e}")
                return []