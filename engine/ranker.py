import math
from typing import Dict


class ScoringEngine:
    def __init__(self):
        # Cuvinte cheie care indică evenimente cu impact "macro"
        self.weights = {
            "war": 40, "revolution": 45, "independence": 50,
            "discovered": 35, "invention": 35, "treaty": 30,
            "assassinated": 40, "empire": 30, "republic": 30,
            "scientific": 35, "atomic": 40, "space": 35
        }

    def heuristic_score(self, item: dict) -> float:
        """Evaluare rapidă bazată pe text și metadate Wikipedia."""
        score = 15.0  # Base score
        text = item.get("text", "").lower()

        # 1. Analiză cuvinte cheie
        for word, bonus in self.weights.items():
            if word in text:
                score += bonus

        # 2. Bonus pentru densitatea informației (câte pagini wiki sunt legate de eveniment)
        pages_count = len(item.get("pages", []))
        score += min(pages_count * 4, 25)

        return min(score, 100.0)

    def calculate_final_score(self, h_score: float, ai_score: float, views: int) -> float:
        """
        Formula Elite:
        - 50% AI (Calitatea subiectului)
        - 30% Popularitate (Logarithmic Views)
        - 20% Heuristică (Metadate Wiki)
        """
        # Scara logaritmică: 100 views -> 10 pct, 10k views -> 20 pct, 1M views -> 30 pct
        log_views = math.log10(views + 1) * 5
        popularity_score = min(log_views, 30.0)

        # Ponderea componentelor
        final_score = (ai_score * 0.5) + popularity_score + (h_score * 0.2)

        return round(final_score, 2)