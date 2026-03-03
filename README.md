# Lokales Störungsmanagement-System

MVP-Implementierung mit FastAPI, SQLite (inkl. FTS5), AD-Test, Live-Suche, dynamischer Eingabemaske, Auswertung und lokaler KI-Prognose-Worker (heuristisch als Platzhalter für Qwen 3.5).

## Start

```bash
python -m venv .venv
source .venv/bin/activate  # unter Windows: .venv\Scripts\activate
pip install -r requirements.txt
python run.py
```

## EXE-Build (Windows)

```bash
pip install pyinstaller
pyinstaller --onefile --name stoerungsserver run.py
```

Danach liegt die Server-EXE in `dist/stoerungsserver.exe`.

## Kernfunktionen

- Login-Pflicht mit lokalem Admin-Fallback (`admin/admin` bei Erststart, Passwortwechsel möglich).
- AD-Authentifizierung konfigurierbar + AD-Testbereich.
- Excel-Import in SQLite.
- Volltextsuche (FTS5), kombinierbare Filter, Pagination-Parameter.
- Dynamische Störungserfassung in 5 Schritten (Linie → Teilanlage → Zeit → Kategorie → Details).
- Auswertung mit Chart.js (Top-Teilanlagen, Kategorien, Trend) + Prognose-Anzeige.
- Preview-Modus (Dummy-Daten nur bei aktivierter Option).
- Hintergrunddienst für Prognose-Refresh in Intervallen.

## Hinweis zur KI/Qwen

Das Modell-Interface ist vorbereitet über `ai.model_path`; die aktuelle `predictor.py` nutzt eine lokale heuristische Berechnung als Runtime-Stub. Eine direkte llama.cpp-/Qwen-Anbindung kann daran ergänzt werden.
