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
        self.blacklist_terms = [
            "country", "republic", "state", "continent", "capital city", 
            "administrative", "municipality", "census-designated", "village",
            "province", "territory", "region", "district"
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
        Extrage DOAR evenimente istorice importante din EXACT ziua de azi
        (aceeași lună și zi, dar din anii trecuți).
        """
        now = datetime.now()
        current_month = now.month
        current_day = now.day
        
        # Confirmăm în log ce dată procesăm
        logger.info(f"🗓️ Fetching historical events for: {current_day:02d}/{current_month:02d}")
        
        url = f"https://en.wikipedia.org/api/rest_v1/feed/onthisday/all/{current_month:02d}/{current_day:02d}"

        async with httpx.AsyncClient(headers=self.headers, timeout=20.0) as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                data = res.json()

                # Folosim DOAR 'selected' - evenimente curate manual
                raw_list = data.get("selected", [])
                
                # Dacă nu avem suficiente, completăm cu 'events'
                if len(raw_list) < 15:
                    additional_events = data.get("events", [])[:20]
                    raw_list.extend(additional_events)

                clean_events = []
                seen_texts = set()  # Evităm duplicate
                
                for ev in raw_list:
                    # VALIDARE 1: Trebuie să aibă an și text
                    year = ev.get("year")
                    text = ev.get("text", "").strip()
                    
                    if not year or not text:
                        continue
                    
                    # VALIDARE 2: Evităm duplicate
                    if text in seen_texts:
                        continue
                    seen_texts.add(text)
                    
                    # VALIDARE 3: Trebuie să aibă cel puțin o pagină Wikipedia asociată
                    pages = ev.get("pages", [])
                    if not pages:
                        continue
                    
                    main_page = pages[0]
                    slug = main_page.get("titles", {}).get("canonical", "")
                    description = main_page.get("description", "").lower()
                    extract = main_page.get("extract", "")
                    
                    # VALIDARE 4: Slug valid
                    if not slug:
                        continue
                    
                    # FILTRU ELITĂ: Eliminăm articole administrative
                    if any(term in description for term in self.blacklist_terms):
                        continue
                    
                    # VALIDARE 5: Evenimentul trebuie să aibă substanță
                    if len(extract) < 50:  # Prea scurt = probabil stub
                        continue
                    
                    # Construim eveniment validat
                    clean_events.append({
                        "year": int(year),
                        "text": text,
                        "slug": slug,
                        "description": main_page.get("description", ""),
                        "extract": extract[:500],  # Primele 500 caractere
                        "thumbnail": main_page.get("thumbnail", {}).get("source") if main_page.get("thumbnail") else None,
                        "month": current_month,
                        "day": current_day
                    })
                
                logger.info(f"✅ Found {len(clean_events)} valid events for {current_day:02d}/{current_month:02d}")
                return clean_events

            except Exception as e:
                logger.error(f"❌ Scraper failed: {e}")
                return []