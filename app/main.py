from __future__ import annotations

import json
from datetime import datetime, timedelta
from threading import Event, Thread

import pandas as pd
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.auth import change_password, ensure_default_admin, get_user_by_session, login, test_ad_connection
from app.database import get_conn, get_setting, init_db, set_setting, utcnow
from app.predictor import run_prediction

app = FastAPI(title="Lokales Störungsmanagement")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
stop_event = Event()


def background_predictor() -> None:
    while not stop_event.is_set():
        if get_setting("ai.enabled", True):
            run_prediction()
        refresh_hours = int(get_setting("ai.refresh_hours", 6))
        stop_event.wait(timeout=max(1, refresh_hours) * 3600)


def current_user(request: Request):
    token = request.cookies.get("session")
    user = get_user_by_session(token) if token else None
    if not user:
        raise HTTPException(status_code=401)
    return user


@app.on_event("startup")
def startup() -> None:
    init_db()
    ensure_default_admin()
    defaults = {
        "preview.enabled": False,
        "evaluation.default_weeks": 6,
        "evaluation.top_n": 10,
        "ai.enabled": True,
        "ai.months": 6,
        "ai.refresh_hours": 6,
        "ai.model_path": "models/qwen",
        "auth.ad": {"enabled": False},
    }
    for key, value in defaults.items():
        if get_setting(key, None) is None:
            set_setting(key, value)
    Thread(target=background_predictor, daemon=True).start()


@app.on_event("shutdown")
def shutdown() -> None:
    stop_event.set()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
def do_login(request: Request, username: str = Form(...), password: str = Form(...)):
    token = login(username, password)
    if not token:
        return templates.TemplateResponse("login.html", {"request": request, "error": "Login fehlgeschlagen"})
    response = RedirectResponse("/app", status_code=302)
    response.set_cookie("session", token, httponly=True)
    return response


@app.get("/app", response_class=HTMLResponse)
def app_shell(request: Request, user=Depends(current_user)):
    return templates.TemplateResponse("app.html", {"request": request, "user": user})


@app.post("/auth/change-password")
def do_change_password(new_password: str = Form(...), user=Depends(current_user)):
    change_password(user.username, new_password)
    return {"status": "ok"}


@app.post("/api/import")
async def import_excel(file: UploadFile = File(...), user=Depends(current_user)):
    df = pd.read_excel(file.file)
    conn = get_conn()
    inserted = 0
    try:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO disturbances(
                  stoerung_text, anlage, teilanlage_id, ursache, behebung, dauer_minuten,
                  kategorie, bereitschaft, berechtigter_einsatz, verantwortlicher, event_time, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row.get("stoerung_text", "")),
                    str(row.get("anlage", "")),
                    row.get("teilanlage_id"),
                    str(row.get("ursache", "")),
                    str(row.get("behebung", "")),
                    int(row.get("dauer_minuten", 0) or 0),
                    str(row.get("kategorie", "")),
                    str(row.get("bereitschaft", "")),
                    str(row.get("berechtigter_einsatz", "")),
                    str(row.get("verantwortlicher", "")),
                    str(row.get("event_time", utcnow())),
                    utcnow(),
                ),
            )
            inserted += 1
        conn.commit()
    finally:
        conn.close()
    return {"inserted": inserted}


@app.get("/api/disturbances")
def search_disturbances(
    q: str = "",
    line_id: int | None = None,
    subplant_id: int | None = None,
    category: str | None = None,
    responsible: str | None = None,
    min_duration: int | None = None,
    max_duration: int | None = None,
    start: str | None = None,
    end: str | None = None,
    page: int = 1,
    page_size: int = 50,
    user=Depends(current_user),
):
    query = """
    SELECT d.*, sp.name as subplant_name
    FROM disturbances d
    LEFT JOIN subplants sp ON sp.id = d.teilanlage_id
    LEFT JOIN line_subplant ls ON ls.subplant_id = d.teilanlage_id
    WHERE 1=1
    """
    params: list = []

    if q:
        query += " AND d.id IN (SELECT rowid FROM disturbances_fts WHERE disturbances_fts MATCH ?)"
        params.append(q)
    if line_id:
        query += " AND ls.line_id = ?"
        params.append(line_id)
    if subplant_id:
        query += " AND d.teilanlage_id = ?"
        params.append(subplant_id)
    if category:
        query += " AND d.kategorie = ?"
        params.append(category)
    if responsible:
        query += " AND d.verantwortlicher LIKE ?"
        params.append(f"%{responsible}%")
    if min_duration is not None:
        query += " AND d.dauer_minuten >= ?"
        params.append(min_duration)
    if max_duration is not None:
        query += " AND d.dauer_minuten <= ?"
        params.append(max_duration)
    if start:
        query += " AND d.event_time >= ?"
        params.append(start)
    if end:
        query += " AND d.event_time <= ?"
        params.append(end)

    query += " ORDER BY d.event_time DESC LIMIT ? OFFSET ?"
    params.extend([page_size, (page - 1) * page_size])

    conn = get_conn()
    try:
        rows = [dict(r) for r in conn.execute(query, params).fetchall()]
        return rows
    finally:
        conn.close()


@app.post("/api/disturbances")
def create_disturbance(payload: dict, user=Depends(current_user)):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO disturbances(stoerung_text, anlage, teilanlage_id, ursache, behebung, dauer_minuten,
            kategorie, bereitschaft, berechtigter_einsatz, verantwortlicher, event_time, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("stoerung_text"),
                payload.get("anlage"),
                payload.get("teilanlage_id"),
                payload.get("ursache"),
                payload.get("behebung"),
                payload.get("dauer_minuten", 0),
                payload.get("kategorie"),
                payload.get("bereitschaft"),
                payload.get("berechtigter_einsatz"),
                payload.get("verantwortlicher"),
                payload.get("event_time", utcnow()),
                utcnow(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok"}


@app.get("/api/master-data")
def master_data(user=Depends(current_user)):
    conn = get_conn()
    try:
        lines = [dict(x) for x in conn.execute("SELECT * FROM lines WHERE active = 1 ORDER BY name").fetchall()]
        subplants = [dict(x) for x in conn.execute("SELECT * FROM subplants WHERE active = 1 ORDER BY name").fetchall()]
        mapping = [dict(x) for x in conn.execute("SELECT * FROM line_subplant").fetchall()]
        return {"lines": lines, "subplants": subplants, "mapping": mapping}
    finally:
        conn.close()


@app.get("/api/analytics")
def analytics(user=Depends(current_user)):
    weeks = int(get_setting("evaluation.default_weeks", 6))
    since = (datetime.utcnow() - timedelta(weeks=weeks)).isoformat()
    conn = get_conn()
    try:
        top = [
            dict(x)
            for x in conn.execute(
                """
                SELECT COALESCE(sp.name, 'Unbekannt') as subplant, COUNT(*) as count,
                       SUM(COALESCE(d.dauer_minuten,0)) as total_duration,
                       AVG(COALESCE(d.dauer_minuten,0)) as avg_duration
                FROM disturbances d
                LEFT JOIN subplants sp ON sp.id = d.teilanlage_id
                WHERE d.event_time >= ?
                GROUP BY COALESCE(sp.name, 'Unbekannt')
                ORDER BY count DESC
                LIMIT ?
                """,
                (since, int(get_setting("evaluation.top_n", 10))),
            ).fetchall()
        ]
        categories = [
            dict(x)
            for x in conn.execute(
                "SELECT COALESCE(kategorie,'Unbekannt') as category, COUNT(*) as count FROM disturbances WHERE event_time >= ? GROUP BY COALESCE(kategorie,'Unbekannt')",
                (since,),
            ).fetchall()
        ]
        trend = [
            dict(x)
            for x in conn.execute(
                "SELECT substr(event_time,1,10) as day, COUNT(*) as count FROM disturbances WHERE event_time >= ? GROUP BY substr(event_time,1,10) ORDER BY day",
                (since,),
            ).fetchall()
        ]
        pred = conn.execute("SELECT * FROM predictions ORDER BY created_at DESC LIMIT 1").fetchone()
        prediction = dict(pred) if pred else None
        if prediction:
            prediction["prediction_json"] = json.loads(prediction["prediction_json"])
        return {"top": top, "categories": categories, "trend": trend, "prediction": prediction}
    finally:
        conn.close()


@app.post("/api/settings/ad-test")
def ad_test(payload: dict, user=Depends(current_user)):
    ok, message = test_ad_connection(
        payload.get("server", ""),
        payload.get("domain", ""),
        payload.get("username", ""),
        payload.get("password", ""),
    )
    return {"ok": ok, "message": message}


@app.post("/api/settings")
def save_settings(payload: dict, user=Depends(current_user)):
    for key, value in payload.items():
        set_setting(key, value)
    return {"status": "ok"}


@app.get("/api/settings")
def list_settings(user=Depends(current_user)):
    keys = [
        "preview.enabled",
        "evaluation.default_weeks",
        "evaluation.top_n",
        "ai.enabled",
        "ai.months",
        "ai.refresh_hours",
        "ai.model_path",
        "auth.ad",
    ]
    return {k: get_setting(k) for k in keys}


@app.post("/api/preview/dummy")
def create_dummy_data(user=Depends(current_user)):
    if not get_setting("preview.enabled", False):
        return JSONResponse({"error": "Preview mode ist deaktiviert"}, status_code=400)

    conn = get_conn()
    try:
        for i in range(30):
            conn.execute(
                "INSERT INTO disturbances(stoerung_text, anlage, ursache, behebung, dauer_minuten, kategorie, verantwortlicher, event_time, created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    f"Dummy Störung {i}",
                    "Preview Anlage",
                    "Simulierte Ursache",
                    "Simulierte Behebung",
                    10 + i,
                    "Preview",
                    "System",
                    (datetime.utcnow() - timedelta(days=i)).isoformat(),
                    utcnow(),
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok", "rows": 30}
