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
        Formula de elite pentru relevanță globală:
        40% AI Analysis (Calitate/Narațiune)
        30% Heuristic (Importanță istorică brută)
        30% Popularity (Page Views - Logarithmic Scale)
        """
        # Normalizăm vizualizările folosind logaritm (baza 10)
        # 100 views -> log10(100) = 2
        # 10.000 views -> log10(10000) = 4
        # 1.000.000 views -> log10(1000000) = 6
        # Înmulțim cu 5 pentru a aduce vizualizările în range-ul 0-30 puncte
        log_views = math.log10(views + 1) * 5
        popularity_score = min(log_views, 30.0)

        # Ponderea AI (0-100) -> 40% = max 40 puncte
        # Ponderea Heuristic (0-100) -> 30% = max 30 puncte
        final_score = (ai_score * 0.4) + (h_score * 0.3) + popularity_score

        return round(final_score, 2)