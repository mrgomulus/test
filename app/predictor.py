from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta

from app.database import get_conn, get_setting, utcnow


def run_prediction() -> dict:
    horizon_months = int(get_setting("ai.months", 6))
    since = (datetime.utcnow() - timedelta(days=30 * horizon_months)).isoformat()

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT d.kategorie, d.ursache, sp.name as subplant, d.event_time
            FROM disturbances d
            LEFT JOIN subplants sp ON sp.id = d.teilanlage_id
            WHERE d.event_time >= ?
            """,
            (since,),
        ).fetchall()

        categories = Counter(r["kategorie"] or "Unbekannt" for r in rows)
        causes = Counter(r["ursache"] or "Unbekannt" for r in rows)
        subplants = Counter(r["subplant"] or "Unbekannt" for r in rows)

        payload = {
            "wo": subplants.most_common(3),
            "was": causes.most_common(3),
            "wann": "Erhöhtes Risiko während der nächsten 7 Tage bei steigendem Trend",
            "kategorie_trend": categories.most_common(5),
            "model": "Qwen 3.5 (placeholder heuristic)",
        }
        conn.execute(
            "INSERT INTO predictions(type, prediction_json, confidence, created_at) VALUES(?,?,?,?)",
            ("wo_wann_was", json.dumps(payload), 0.62, utcnow()),
        )
        conn.commit()
    finally:
        conn.close()
    return payload
