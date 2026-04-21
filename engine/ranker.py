import math
from schema.models import EventCategory


# Category weight — how inherently engaging each topic is for a "Today in History" app
CATEGORY_WEIGHTS = {
    # ── Free-tier categories ──
    "science_discovery":  1.20,   # Space, medicine, computers — always viral
    "tech_innovation":    1.20,   # Inventions, breakthroughs
    "exploration":        1.15,   # First steps, discoveries
    "war_conflict":       1.10,   # Revolutions, treaties, major conflicts
    "politics_state":     1.10,   # Elections, regime changes
    "natural_disaster":   1.05,   # Earthquakes, floods — emotional pull
    "religion_phil":      1.00,   # Religious/philosophical milestones
    "culture_arts":       1.00,   # Default baseline

    # ── PRO-only categories ──
    # These are weighted lower because PRO ranks against itself, not against
    # free events. The weight only matters inside its own bucket.
    "personalities":      1.10,   # Births/deaths of iconic figures — very shareable
    "media":              1.05,   # Film premieres, album releases, TV firsts
    "sport":              1.00,   # Olympic moments, record breaks, finals
}


class ScoringEngine:
    def __init__(self):
        pass

    def get_category_weight(self, category: str) -> float:
        """Return engagement multiplier for the event's category."""
        return CATEGORY_WEIGHTS.get(category, 1.00)

    def views_component(self, views: int) -> float:
        """
        Logarithmic Wikipedia pageviews component.
        - 0 views       → 0.0
        - 1,000 views   → ~24.0
        - 10,000 views  → ~32.0
        - 100,000 views → ~40.0  (hard cap)
        """
        return min(math.log10(views + 1) * 8, 40.0)

    def recency_bonus(self, year: int) -> float:
        """
        Slight bonus for events close to round anniversaries (25, 50, 75, 100...).
        Max bonus: 5 points.
        """
        if not year or year <= 0:
            return 0.0

        current_year = 2026
        age = current_year - year
        if age <= 0:
            return 0.0

        remainder = age % 25
        if remainder == 0:
            return 5.0
        elif remainder <= 2 or remainder >= 23:
            return 2.0
        return 0.0

    def calculate_final_score(
        self,
        ai_score: float,
        views: int,
        category: str = "culture_arts",
        year: int = 0
    ) -> float:
        """
        Final Score Formula — optimized for "Today in History" engagement.

        Components:
          70% → AI Score     (historical significance + content quality)
          20% → Views        (logarithmic Wikipedia traffic)
          10% → Category     (inherent virality weight of the topic)
          + Recency Bonus    (anniversary effect, max +5)
        """
        # 1. AI component (0–70 points)
        ai_component = ai_score * 0.70

        # 2. Popularity component (0–20 points, scaled from 0–40 raw)
        raw_views = self.views_component(views)
        views_component = raw_views * 0.50

        # 3. Category component (0–10 points)
        cat_weight = self.get_category_weight(category)
        # Normalize: weights range 1.00–1.20, so we map that range to 0–10
        category_component = max(0.0, (cat_weight - 1.00) / (1.20 - 1.00) * 10)

        # 4. Base score + bonus
        base = ai_component + views_component + category_component
        bonus = self.recency_bonus(year)

        final = base + bonus
        return round(min(final, 100.0), 2)