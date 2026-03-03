from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Optional

from ldap3 import ALL, Connection, Server
from passlib.context import CryptContext

from app.database import get_conn, get_setting

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
_sessions: dict[str, str] = {}


@dataclass
class User:
    username: str
    role: str
    auth_type: str
    must_change_password: bool = False


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)


def ensure_default_admin() -> None:
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()
        if not row:
            conn.execute(
                "INSERT INTO users(username, password_hash, role, auth_type, must_change_password) VALUES(?,?,?,?,1)",
                ("admin", hash_password("admin"), "admin", "local"),
            )
            conn.commit()
    finally:
        conn.close()


def authenticate_local(username: str, password: str) -> Optional[User]:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT username, password_hash, role, auth_type, must_change_password FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if not row or row["auth_type"] != "local":
            return None
        if not row["password_hash"] or not verify_password(password, row["password_hash"]):
            return None
        return User(
            username=row["username"],
            role=row["role"],
            auth_type=row["auth_type"],
            must_change_password=bool(row["must_change_password"]),
        )
    finally:
        conn.close()


def authenticate_ad(username: str, password: str) -> Optional[User]:
    cfg = get_setting("auth.ad", {})
    if not cfg or not cfg.get("enabled"):
        return None

    server = Server(cfg.get("server"), get_info=ALL)
    user_dn = f"{cfg.get('domain')}\\{username}" if cfg.get("domain") else username
    try:
        with Connection(server, user=user_dn, password=password, auto_bind=True):
            pass
    except Exception:
        return None

    conn = get_conn()
    try:
        mapped = conn.execute(
            "SELECT role FROM users WHERE username = ? AND auth_type = 'ad'", (username,)
        ).fetchone()
        role = mapped["role"] if mapped else "user"
    finally:
        conn.close()
    return User(username=username, role=role, auth_type="ad")


def login(username: str, password: str) -> Optional[str]:
    user = authenticate_local(username, password) or authenticate_ad(username, password)
    if not user:
        return None
    token = secrets.token_urlsafe(32)
    _sessions[token] = user.username
    return token


def get_user_by_session(token: str) -> Optional[User]:
    username = _sessions.get(token)
    if not username:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT username, role, auth_type, must_change_password FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if row:
            return User(
                username=row["username"],
                role=row["role"],
                auth_type=row["auth_type"],
                must_change_password=bool(row["must_change_password"]),
            )
    finally:
        conn.close()
    return User(username=username, role="user", auth_type="ad")


def change_password(username: str, new_password: str) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET password_hash = ?, must_change_password = 0 WHERE username = ?",
            (hash_password(new_password), username),
        )
        conn.commit()
    finally:
        conn.close()


def test_ad_connection(server: str, domain: str, username: str, password: str) -> tuple[bool, str]:
    try:
        ldap_server = Server(server, get_info=ALL)
        user_dn = f"{domain}\\{username}" if domain else username
        with Connection(ldap_server, user=user_dn, password=password, auto_bind=True):
            return True, "Verbindung erfolgreich, Benutzer gefunden"
    except Exception as exc:
        return False, f"Fehler: {exc}"
