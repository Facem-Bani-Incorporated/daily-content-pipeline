import os
import psycopg2
from difflib import SequenceMatcher
from core.logger import setup_logger

logger = setup_logger("Deduplicator")


class EventDeduplicator:
    """
    Prevents the same historical event from being sent to the backend
    multiple times — across different days, categories, or tiers.

    Matches on:
      1. Exact source_url (Wikipedia slug) — strongest signal
      2. Same year + fuzzy title match (>= threshold similarity)
      3. Same slug already selected in this pipeline run (cross-tier, intra-run)

    Connection strategy (in priority order):
      1. DATABASE_URL  (preferred — single connection string)
      2. PG* env vars  (fallback — PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE)
    """

    def __init__(self, similarity_threshold: float = 0.85):
        self.threshold = similarity_threshold
        self._existing_cache = None  # lazy-loaded once per pipeline run
        # Tracks slugs kept during this pipeline run so FREE and PRO
        # don't independently select the same Wikipedia article.
        self._current_run_slugs: set = set()

    def _get_connection(self):
        # Strategy 1: DATABASE_URL (Railway "Add Reference" puts it here)
        db_url = os.environ.get("DATABASE_URL")
        if db_url:
            return psycopg2.connect(db_url)

        # Strategy 2: build from PG* env vars (fallback)
        pg_host = os.environ.get("PGHOST")
        pg_port = os.environ.get("PGPORT", "5432")
        pg_user = os.environ.get("PGUSER")
        pg_password = os.environ.get("PGPASSWORD")
        pg_database = os.environ.get("PGDATABASE")

        if all([pg_host, pg_user, pg_password, pg_database]):
            logger.info(f"📡 Using PG* vars to connect (host={pg_host})")
            return psycopg2.connect(
                host=pg_host,
                port=pg_port,
                user=pg_user,
                password=pg_password,
                dbname=pg_database,
            )

        raise RuntimeError(
            "No database credentials found. Set DATABASE_URL or "
            "PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE in environment."
        )

    def _load_existing_events(self) -> list:
        """
        Load all existing events from DB once.
        Returns list of (source_url, year, title_en) tuples.
        """
        if self._existing_cache is not None:
            return self._existing_cache

        try:
            conn = self._get_connection()
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT source_url,
                           EXTRACT(YEAR FROM event_date)::int AS year,
                           title_translations->>'en' AS title_en
                    FROM events
                """)
                rows = cur.fetchall()
            except Exception as schema_err:
                logger.warning(
                    f"⚠️ Primary dedup query failed ({schema_err}) — trying fallback schema"
                )
                conn.rollback()
                cur.execute("""
                    SELECT source_url,
                           EXTRACT(YEAR FROM event_date)::int AS year,
                           NULL AS title_en
                    FROM events
                """)
                rows = cur.fetchall()

            cur.close()
            conn.close()
            self._existing_cache = rows
            logger.info(f"📚 Loaded {len(rows)} existing events from DB for dedup check")
            return rows
        except Exception as e:
            logger.error(f"🚨 Failed to load existing events: {e}")
            self._existing_cache = []
            return []

    def _normalize_url(self, slug: str) -> str:
        if not slug:
            return ""
        return f"https://en.wikipedia.org/wiki/{slug}"

    def _is_duplicate(self, item: dict, existing: list) -> tuple:
        slug = item.get("slug", "")
        year = item.get("year", 0)
        title_en = (
            (item.get("titles") or {}).get("en")
            or item.get("text", "")[:80]
        )
        title_en = (title_en or "").strip()
        candidate_url = self._normalize_url(slug)

        # Fast check: same Wikipedia article already selected in this run
        if slug and slug.lower() in self._current_run_slugs:
            return True, f"Already selected in this pipeline run: {slug}"

        for existing_url, existing_year, existing_title in existing:
            if existing_url and candidate_url and candidate_url == existing_url:
                return True, f"Same Wikipedia URL: {slug}"

            if (
                existing_year == year
                and existing_title
                and title_en
                and year > 0
            ):
                ratio = SequenceMatcher(
                    None,
                    title_en.lower(),
                    existing_title.lower().strip()
                ).ratio()
                if ratio >= self.threshold:
                    return True, (
                        f"Fuzzy match ({ratio:.0%}) with: "
                        f"{existing_title[:60]}"
                    )

        return False, ""

    def has_events_for_date(self, target_date) -> bool:
        """Check if the DB already has events for this calendar day (month+day)."""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE EXTRACT(MONTH FROM event_date) = %s
                  AND EXTRACT(DAY FROM event_date) = %s
                """,
                (target_date.month, target_date.day),
            )
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            logger.info(
                f"📅 Date check {target_date}: {count} events found in DB"
            )
            return count > 0
        except Exception as e:
            logger.warning(f"⚠️ Could not check events for {target_date}: {e} — will process anyway")
            return False

    def filter_duplicates(self, events: list, tier: str = "FREE") -> list:
        if not events:
            return []

        existing = self._load_existing_events()
        if not existing:
            logger.warning(
                f"⚠️ No existing events in DB — skipping dedup for {tier} (first run?)"
            )
            return events

        kept = []
        skipped_count = 0
        for item in events:
            is_dup, reason = self._is_duplicate(item, existing)
            if is_dup:
                skipped_count += 1
                logger.warning(
                    f"🚫 [{tier}] Duplicate skipped: {item.get('year')} "
                    f"{item.get('slug', '')} — {reason}"
                )
            else:
                slug_norm = (item.get("slug") or "").lower()
                if slug_norm:
                    self._current_run_slugs.add(slug_norm)
                kept.append(item)

        logger.info(
            f"✅ [{tier}] Dedup: {len(kept)} kept, {skipped_count} skipped "
            f"(out of {len(events)})"
        )
        return kept

    def filter_final_cross_tier(self, events: list) -> list:
        """
        Final dedup on the combined FREE+PRO EventDetail list.
        Called after free_events + pro_events are merged, before sorting.
        Events must be ordered FREE-first so PRO duplicates are dropped, not FREE.

        Drops any event that duplicates an already-kept event by:
          1. Exact source URL (same Wikipedia article)
          2. Same year + fuzzy EN title similarity >= 0.72 (same historical
             moment described via different Wikipedia articles)
        """
        seen_urls: set = set()
        seen_for_fuzzy: list = []  # (year, title_en_lower, event) tuples
        kept = []

        for ev in events:
            url = str(ev.source_url)
            year = ev.year
            title_en = (ev.title_translations.en or "").strip().lower()

            # 1. Exact URL
            if url in seen_urls:
                logger.warning(
                    f"🔁 FINAL exact dup dropped [{ev.category.value}]: "
                    f"{ev.title_translations.en[:60]}"
                )
                continue

            # 2. Same year + fuzzy title
            is_fuzzy_dup = False
            for seen_year, seen_title, seen_ev in seen_for_fuzzy:
                if seen_year != year or not title_en or not seen_title:
                    continue
                ratio = SequenceMatcher(None, title_en, seen_title).ratio()
                if ratio >= 0.72:
                    logger.warning(
                        f"🔁 FINAL fuzzy dup dropped [{ev.category.value}] "
                        f"'{ev.title_translations.en[:50]}' "
                        f"({ratio:.0%} similar to [{seen_ev.category.value}] "
                        f"'{seen_ev.title_translations.en[:50]}')"
                    )
                    is_fuzzy_dup = True
                    break

            if is_fuzzy_dup:
                continue

            seen_urls.add(url)
            seen_for_fuzzy.append((year, title_en, ev))
            kept.append(ev)

        dropped = len(events) - len(kept)
        if dropped:
            logger.info(
                f"🧹 FINAL cross-tier dedup: {len(kept)} kept, {dropped} dropped"
            )
        return kept