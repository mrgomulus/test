from app.database import init_db, get_conn


def test_tables_exist():
    init_db()
    conn = get_conn()
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert 'disturbances' in names
    assert 'disturbances_fts' in names
    assert 'predictions' in names
