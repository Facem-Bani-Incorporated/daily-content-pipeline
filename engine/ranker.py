import math
from typing import Dict

class ScoringEngine:
    def __init__(self):
        # Cuvinte cheie care indică evenimente cu potențial uriaș de click/engagement
        self.weights = {
            # Drama & Conflict (High Engagement)
            "war": 40, "revolution": 50, "assassinated": 50, "execution": 45,
            # Triumf & Progres
            "discovered": 40, "invention": 40, "space": 45, "atomic": 45,
            # Shock Value & Curiozitate
            "scandal": 50, "disaster": 45, "mystery": 40, "secret": 40,
            # Istorie Majoră
            "independence": 40, "empire": 35, "collapse": 45
        }

    def heuristic_score(self, item: dict) -> float:
        """Evaluare bazată pe triggeri emoționali și densitatea informației."""
        score = 10.0  # Base score
        text = item.get("text", "").lower()

        # 1. Analiză cuvinte cheie (căutăm factorul "wow")
        for word, bonus in self.weights.items():
            if word in text:
                score += bonus

        # 2. Bonus pentru densitatea informației (câte pagini wiki sunt legate)
        # Lumea scrie mult despre lucruri importante
        pages_count = len(item.get("pages", []))
        score += min(pages_count * 5, 30)

        return min(score, 100.0)

    def calculate_final_score(self, h_score: float, ai_score: float, views: int) -> float:
        """
        Formula Optimizată pentru Retenție:
        - 40% AI (Calitatea subiectului și potențialul de hook)
        - 40% Popularitate (Logarithmic Views pe Wikipedia - Dacă e popular acolo, va fi și în app)
        - 20% Heuristică (Triggeri emoționali)
        """
        # Amplificăm importanța vizualizărilor.
        # Logaritmul asigură că 10.000 de views nu zdrobește un eveniment cu 1.000,
        # dar totuși îi dă un avantaj clar.
        log_views = math.log10(views + 1) * 8
        popularity_score = min(log_views, 40.0) # Maxim 40 puncte din views

        # Ponderea componentelor ajustată
        final_score = (ai_score * 0.40) + popularity_score + (h_score * 0.20)

        return round(final_score, 2)