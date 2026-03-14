import math
from schema.models import EventCategory


# Category weight — how inherently engaging each topic is for a "Today in History" app
CATEGORY_WEIGHTS = {
    "science_technology": 1.20,   # Space, medicine, computers — always viral
    "exploration":        1.15,   # First steps, discoveries
    "politics_war":       1.10,   # Revolutions, treaties, major conflicts
    "social_culture":     1.05,   # Civil rights, cultural milestones
    "culture_arts":       1.00,   # Default baseline
    "economics":          0.95,   # Important but less shareable
    "sports":             0.85,   # Niche unless legendary
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
        
        Using log10 with a scale factor of 8 gives meaningful separation
        without letting mega-popular articles completely dominate.
        """
        return min(math.log10(views + 1) * 8, 40.0)

    def recency_bonus(self, year: int) -> float:
        """
        Slight bonus for events close to round anniversaries (50, 100, 150...).
        People love milestone anniversaries — they get shared more.
        Max bonus: 5 points.
        """
        if not year or year <= 0:
            return 0.0
        
        current_year = 2026
        age = current_year - year
        if age <= 0:
            return 0.0

        # Check if this year is a round anniversary (multiple of 25)
        remainder = age % 25
        if remainder == 0:
            return 5.0
        elif remainder <= 2 or remainder >= 23:
            # Close to a round anniversary (within 2 years)
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
        Final Score Formula — optimized for "Today in History" engagement:

        Components:
          70% → AI Score     (historical significance + content quality + modern relevance)
          20% → Views        (logarithmic Wikipedia traffic — proven public interest)
          10% → Category     (inherent virality weight of the topic)

        Then apply:
          + Recency Bonus    (anniversary effect, max +5)

        Rationale:
          - AI is the primary judge because it understands context, narrative potential,
            and why an event matters — not just raw popularity.
          - Views validate that real humans find the topic interesting (anti-hallucination check).
          - Category weight amplifies events that naturally perform better in mobile apps.
          - Anniversary bonus captures the "on this day X years ago" emotional hook.
        """
        # 1. AI component (0–70 points)
        ai_component = ai_score * 0.70

        # 2. Popularity component (0–20 points, scaled from 0–40 raw)
        raw_views = self.views_component(views)
        views_component = raw_views * 0.50  # Scale 0–40 → 0–20

        # 3. Category component (0–10 points)
        cat_weight = self.get_category_weight(category)
        category_component = (cat_weight - 0.85) / (1.20 - 0.85) * 10  # Normalize to 0–10

        # 4. Base score
        base = ai_component + views_component + category_component

        # 5. Anniversary bonus
        bonus = self.recency_bonus(year)

        final = base + bonus
        return round(min(final, 100.0), 2)