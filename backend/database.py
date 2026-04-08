"""
Database abstraction layer.
Uses Postgres when DATABASE_URL is set (production), SQLite otherwise (local dev).
"""
import os
import sqlite3

DATABASE_URL = os.environ.get('DATABASE_URL', '')

# Postgres support (optional dependency)
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

USE_POSTGRES = bool(DATABASE_URL) and HAS_PSYCOPG2


class DBConnection:
    """Thin wrapper so SQLite and Postgres have the same interface."""

    def __init__(self, conn, is_postgres=False):
        self._conn = conn
        self._is_postgres = is_postgres

    def execute(self, sql, params=None):
        if self._is_postgres:
            sql = _pg_sql(sql)
            cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = self._conn.cursor()
        cur.execute(sql, params or ())
        return CursorWrapper(cur, self._is_postgres)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


class CursorWrapper:
    """Wrap cursor so fetchone/fetchall return dict-like objects for both backends."""

    def __init__(self, cursor, is_postgres):
        self._cursor = cursor
        self._is_postgres = is_postgres

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._is_postgres:
            return row  # RealDictCursor already returns dicts
        return row  # sqlite3.Row supports dict-style access

    def fetchall(self):
        rows = self._cursor.fetchall()
        if self._is_postgres:
            return rows
        return rows


def _pg_sql(sql):
    """Convert SQLite-flavored SQL to Postgres-compatible SQL."""
    # Replace ? placeholders with %s
    return sql.replace('?', '%s')


def get_db():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        return DBConnection(conn, is_postgres=True)
    else:
        db_path = os.path.join(os.path.dirname(__file__), 'fantasy_golf.db')
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return DBConnection(conn, is_postgres=False)


def init_db():
    if USE_POSTGRES:
        _init_postgres()
    else:
        _init_sqlite()


def _init_sqlite():
    db_path = os.path.join(os.path.dirname(__file__), 'fantasy_golf.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS conferences (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS divisions (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            conference_id INTEGER NOT NULL REFERENCES conferences(id)
        );
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            pin TEXT,
            division_id INTEGER NOT NULL REFERENCES divisions(id),
            active INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY,
            week_number INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            location TEXT,
            start_date TEXT,
            is_major INTEGER DEFAULT 0,
            espn_id TEXT,
            completed INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS picks (
            id INTEGER PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id),
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            pick1 TEXT NOT NULL,
            pick2 TEXT NOT NULL,
            pick3 TEXT NOT NULL,
            pick4 TEXT NOT NULL,
            alternate TEXT,
            submitted_at TEXT DEFAULT (datetime('now')),
            UNIQUE(player_id, tournament_id)
        );
        CREATE TABLE IF NOT EXISTS golfer_results (
            id INTEGER PRIMARY KEY,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            golfer_name TEXT NOT NULL,
            finish_position INTEGER,
            score_to_par INTEGER,
            is_winner INTEGER DEFAULT 0,
            missed_cut INTEGER DEFAULT 0,
            withdrawn INTEGER DEFAULT 0,
            UNIQUE(tournament_id, golfer_name)
        );
        CREATE TABLE IF NOT EXISTS weekly_scores (
            id INTEGER PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id),
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            raw_score REAL,
            winner_bonus REAL DEFAULT 0,
            final_score REAL,
            UNIQUE(player_id, tournament_id)
        );
        CREATE TABLE IF NOT EXISTS weekly_winners (
            id INTEGER PRIMARY KEY,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            division_id INTEGER NOT NULL REFERENCES divisions(id),
            player_id INTEGER NOT NULL REFERENCES players(id),
            score REAL,
            winnings REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        INSERT OR IGNORE INTO settings (key, value) VALUES ('winner_bonus', '2.0');
        INSERT OR IGNORE INTO settings (key, value) VALUES ('missed_cut_penalty', '80');
        INSERT OR IGNORE INTO settings (key, value) VALUES ('weekly_payout', '100');
        INSERT OR IGNORE INTO settings (key, value) VALUES ('major_payout', '200');
    ''')
    conn.commit()
    conn.close()
    print("SQLite database initialized.")


def _init_postgres():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS conferences (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS divisions (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            conference_id INTEGER NOT NULL REFERENCES conferences(id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            pin TEXT,
            division_id INTEGER NOT NULL REFERENCES divisions(id),
            active INTEGER DEFAULT 1
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tournaments (
            id SERIAL PRIMARY KEY,
            week_number INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            location TEXT,
            start_date TEXT,
            is_major INTEGER DEFAULT 0,
            espn_id TEXT,
            completed INTEGER DEFAULT 0
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS picks (
            id SERIAL PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id),
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            pick1 TEXT NOT NULL,
            pick2 TEXT NOT NULL,
            pick3 TEXT NOT NULL,
            pick4 TEXT NOT NULL,
            alternate TEXT,
            submitted_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(player_id, tournament_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS golfer_results (
            id SERIAL PRIMARY KEY,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            golfer_name TEXT NOT NULL,
            finish_position INTEGER,
            score_to_par INTEGER,
            is_winner INTEGER DEFAULT 0,
            missed_cut INTEGER DEFAULT 0,
            withdrawn INTEGER DEFAULT 0,
            UNIQUE(tournament_id, golfer_name)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weekly_scores (
            id SERIAL PRIMARY KEY,
            player_id INTEGER NOT NULL REFERENCES players(id),
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            raw_score REAL,
            winner_bonus REAL DEFAULT 0,
            final_score REAL,
            UNIQUE(player_id, tournament_id)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS weekly_winners (
            id SERIAL PRIMARY KEY,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id),
            division_id INTEGER NOT NULL REFERENCES divisions(id),
            player_id INTEGER NOT NULL REFERENCES players(id),
            score REAL,
            winnings REAL DEFAULT 0
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    cur.execute("INSERT INTO settings (key, value) VALUES ('winner_bonus', '2.0') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO settings (key, value) VALUES ('missed_cut_penalty', '80') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO settings (key, value) VALUES ('weekly_payout', '100') ON CONFLICT (key) DO NOTHING")
    cur.execute("INSERT INTO settings (key, value) VALUES ('major_payout', '200') ON CONFLICT (key) DO NOTHING")

    conn.commit()
    conn.close()
    print("Postgres database initialized.")


if __name__ == '__main__':
    init_db()
