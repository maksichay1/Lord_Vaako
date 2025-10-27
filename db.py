import sqlite3

def get_conn(db_path='streams.db'):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path='streams.db'):
    conn = get_conn(db_path)
    with conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            status TEXT,
            created_at TEXT,
            proxies TEXT,
            categories TEXT,
            cities TEXT,
            collected_links INTEGER DEFAULT 0,
            collected_items INTEGER DEFAULT 0,
            filename TEXT,
            expire_at TEXT,
            meta TEXT
        )
        ''')
    return conn
