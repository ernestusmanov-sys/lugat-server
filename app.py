"""
Lugat — Центральный сервер краудсорсинга (SQLite + GitHub persistence)
"""
import os
import hashlib
import sqlite3
import json as _json
import base64 as _b64
from pathlib import Path
from functools import wraps
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR    = Path(__file__).parent
DICT_PATH   = BASE_DIR / "dictionary.db"
SERVER_DB   = BASE_DIR / "server.db"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "lugat_admin_2024")
SERVER_SALT = "lugat_server_users_2024"

# GitHub-персистентность пользователей
def _tok():
    _e = b'\x04\x08\x1a\x04\x1c=3\x05\x06\x15+n\x03spm*+/-Y\x0b\r\r4\x005\x08agJ^S>"\x01_2;-WQM\x06~UBr\x0b \x02\x0b.)),P4\x1d\x1a{{V{\x0e\x03%\x18%\x16\x00/-R3fC\x03``-;]!8m"A \x02A\x1d^'
    _k = b'canli_lugat_2024'
    return ''.join(chr(b ^ _k[i % len(_k)]) for i, b in enumerate(_e))
_GH_TOKEN = os.environ.get("GH_TOKEN") or _tok()
_GH_REPO  = os.environ.get("GH_REPO", "ernestusmanov-sys/lugat-server")
_GH_FILE  = "server_data/users.json"


def _gh_headers():
    return {
        "Authorization": f"token {_GH_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Lugat/1.0",
    }


def _gh_load_users() -> list:
    """Загружает пользователей из GitHub. Возвращает [] при ошибке."""
    try:
        url = f"https://raw.githubusercontent.com/{_GH_REPO}/main/{_GH_FILE}"
        with urlopen(Request(url, headers={"User-Agent": "Lugat/1.0"}), timeout=15) as r:
            return _json.loads(r.read()).get("users", [])
    except Exception:
        return []


def _gh_save_users(users: list):
    """Сохраняет список пользователей в GitHub."""
    if not _GH_TOKEN:
        return
    try:
        url = f"https://api.github.com/repos/{_GH_REPO}/contents/{_GH_FILE}"
        # Получаем текущий SHA файла
        with urlopen(Request(url, headers=_gh_headers()), timeout=10) as r:
            current = _json.loads(r.read())
        sha = current["sha"]
        content = _json.dumps({"users": users}, ensure_ascii=False, indent=2)
        body = _json.dumps({
            "message": "chore: update server users",
            "content": _b64.b64encode(content.encode()).decode(),
            "sha": sha,
        }).encode()
        req = Request(url, data=body,
                      headers={**_gh_headers(), "Content-Type": "application/json"},
                      method="PUT")
        urlopen(req, timeout=15)
    except Exception:
        pass


def _gh_load_extra(table: str) -> list:
    """Загружает phraseology или collocations из GitHub (raw URL — без лимита размера)."""
    try:
        url = f"https://raw.githubusercontent.com/{_GH_REPO}/main/server_data/{table}.json"
        with urlopen(Request(url, headers={"User-Agent": "Lugat/1.0"}), timeout=30) as r:
            return _json.loads(r.read()).get("rows", [])
    except Exception:
        return []


def _gh_save_extra(table: str, rows: list):
    """Сохраняет phraseology или collocations в GitHub."""
    if not _GH_TOKEN:
        return
    try:
        url = f"https://api.github.com/repos/{_GH_REPO}/contents/server_data/{table}.json"
        sha = None
        try:
            with urlopen(Request(url, headers=_gh_headers()), timeout=10) as r:
                sha = _json.loads(r.read())["sha"]
        except Exception:
            pass
        content = _json.dumps({"rows": rows}, ensure_ascii=False, indent=2)
        payload = {"message": f"chore: update {table}", "content": _b64.b64encode(content.encode()).decode()}
        if sha:
            payload["sha"] = sha
        body = _json.dumps(payload).encode()
        req = Request(url, data=body, headers={**_gh_headers(), "Content-Type": "application/json"}, method="PUT")
        urlopen(req, timeout=20)
    except Exception:
        pass


def _hash_pw(password: str) -> str:
    return hashlib.sha256(f"{SERVER_SALT}{password}".encode()).hexdigest()


def _row(r) -> dict:
    if r is None:
        return None
    return dict(r)


def _rows(rs) -> list:
    return [dict(r) for r in rs]


# ── Database ──────────────────────────────────────────────────────────────────

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SERVER_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = _connect()
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        try:
            db.close()
        except Exception:
            pass


def init_db():
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pending_words (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            word          TEXT NOT NULL,
            translation   TEXT NOT NULL,
            example       TEXT DEFAULT '',
            locality      TEXT DEFAULT '',
            speaker       TEXT DEFAULT '',
            contributor   TEXT DEFAULT '',
            contact       TEXT DEFAULT '',
            direction     TEXT DEFAULT 'ct_ru',
            status        TEXT DEFAULT 'pending',
            reject_reason TEXT DEFAULT '',
            submitted_at  TEXT DEFAULT (datetime('now')),
            moderated_at  TEXT DEFAULT NULL,
            ip_address    TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_words(status);

        CREATE TABLE IF NOT EXISTS moderation_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id    INTEGER,
            action     TEXT,
            admin_note TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS dict_versions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            version       INTEGER NOT NULL UNIQUE,
            words_count   INTEGER DEFAULT 0,
            db_size_bytes INTEGER DEFAULT 0,
            db_hash       TEXT DEFAULT '',
            released_at   TEXT DEFAULT (datetime('now')),
            release_note  TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS notifications (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT NOT NULL,
            message    TEXT NOT NULL,
            target     TEXT DEFAULT 'all',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS server_users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            username      TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role          TEXT DEFAULT 'editor',
            full_name     TEXT DEFAULT '',
            is_active     INTEGER DEFAULT 1,
            created_at    TEXT DEFAULT (datetime('now')),
            last_login    TEXT DEFAULT NULL
        );

        CREATE TABLE IF NOT EXISTS remote_users (
            username      TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role          TEXT DEFAULT 'editor',
            synced_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS app_releases (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            version      TEXT NOT NULL,
            download_url TEXT NOT NULL,
            changelog    TEXT DEFAULT '',
            released_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS feedback (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            message      TEXT NOT NULL,
            contact      TEXT DEFAULT '',
            device_id    TEXT DEFAULT '',
            submitted_at TEXT DEFAULT (datetime('now')),
            ip_address   TEXT DEFAULT '',
            is_read      INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS feedback_replies (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            feedback_id  INTEGER NOT NULL,
            device_id    TEXT NOT NULL,
            reply        TEXT NOT NULL,
            created_at   TEXT DEFAULT (datetime('now')),
            is_delivered INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_replies_device ON feedback_replies(device_id, is_delivered);

        CREATE TABLE IF NOT EXISTS phraseology (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            crimean_tatar TEXT NOT NULL,
            russian       TEXT NOT NULL,
            definition    TEXT DEFAULT '',
            example       TEXT DEFAULT '',
            category      TEXT DEFAULT '',
            is_verified   INTEGER DEFAULT 0,
            entry_lang    TEXT DEFAULT 'ct',
            created_at    TEXT DEFAULT (datetime('now')),
            updated_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS collocations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            crimean_tatar TEXT NOT NULL,
            russian       TEXT NOT NULL,
            definition    TEXT DEFAULT '',
            example       TEXT DEFAULT '',
            category      TEXT DEFAULT '',
            is_verified   INTEGER DEFAULT 0,
            entry_lang    TEXT DEFAULT 'ct',
            created_at    TEXT DEFAULT (datetime('now')),
            updated_at    TEXT DEFAULT (datetime('now'))
        );
    """)
    # Initial dict version if dictionary.db is present and no versions exist
    if DICT_PATH.exists():
        count = conn.execute("SELECT COUNT(*) FROM dict_versions").fetchone()[0]
        if count == 0:
            wc = _word_count()
            conn.execute(
                "INSERT INTO dict_versions (version, words_count, db_size_bytes, db_hash, release_note) "
                "VALUES (1, ?, ?, ?, 'Начальная версия')",
                (wc, DICT_PATH.stat().st_size, _file_hash(DICT_PATH))
            )
    # Загружаем пользователей из GitHub (persistent storage)
    gh_users = _gh_load_users()
    for u in gh_users:
        uname = u.get("username", "").strip()
        phash = u.get("password_hash", "").strip()
        role  = u.get("role", "editor")
        fname = u.get("full_name", "")
        active = u.get("is_active", 1)
        if not uname or not phash:
            continue
        conn.execute("""
            INSERT INTO server_users (username, password_hash, role, full_name, is_active)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                password_hash = excluded.password_hash,
                role          = excluded.role,
                full_name     = excluded.full_name,
                is_active     = excluded.is_active
        """, (uname, phash, role, fname, active))
    conn.commit()

    # Загружаем phraseology и collocations из GitHub
    for table in ("phraseology", "collocations"):
        rows = _gh_load_extra(table)
        if rows:
            conn.execute(f"DELETE FROM {table}")
            for r in rows:
                conn.execute(
                    f"INSERT INTO {table} (crimean_tatar, russian, definition, example, "
                    f"category, is_verified, entry_lang) VALUES (?,?,?,?,?,?,?)",
                    (r.get("crimean_tatar",""), r.get("russian",""),
                     r.get("definition",""), r.get("example",""),
                     r.get("category",""), r.get("is_verified",0),
                     r.get("entry_lang","ct"))
                )
    conn.commit()
    conn.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _word_count():
    if not DICT_PATH.exists():
        return 0
    try:
        c = sqlite3.connect(DICT_PATH)
        n = c.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        c.close()
        return n
    except Exception:
        return 0


def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = (
            request.headers.get("X-Admin-Token") or
            request.args.get("token") or
            (request.get_json(silent=True) or {}).get("token")
        )
        if token != ADMIN_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper


# ── Публичные ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"name": "Lugat API", "version": "1.0", "status": "running"})


@app.route("/api/version")
def get_version():
    db  = get_db()
    row = db.execute(
        "SELECT version, words_count, db_size_bytes, db_hash, released_at "
        "FROM dict_versions ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if not row:
        return jsonify({"version": 0, "words_count": 0, "db_ready": False})
    r = dict(row)
    return jsonify({
        "version":     r["version"],
        "words_count": r["words_count"],
        "db_size":     r["db_size_bytes"],
        "db_hash":     r["db_hash"],
        "released_at": r["released_at"] or "",
        "db_ready":    DICT_PATH.exists(),
    })


@app.route("/api/updates")
def get_updates():
    client_version = int(request.args.get("client_version", 0))
    client_hash    = request.args.get("client_hash", "")
    db  = get_db()
    row = db.execute(
        "SELECT version, db_hash FROM dict_versions ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if not row or not DICT_PATH.exists():
        return jsonify({"up_to_date": True, "version": 0, "db_ready": False})
    r = dict(row)
    if client_version >= r["version"] and client_hash == r["db_hash"]:
        return jsonify({"up_to_date": True, "version": r["version"]})
    return send_file(
        DICT_PATH,
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name="dictionary.db",
    )


@app.route("/api/submit", methods=["POST"])
def submit_word():
    data        = request.get_json(force=True, silent=True) or {}
    word        = str(data.get("word",        "")).strip()
    translation = str(data.get("translation", "")).strip()
    if not word or not translation:
        return jsonify({"error": "Слово и перевод обязательны"}), 400
    ip  = request.remote_addr or ""
    db  = get_db()
    dup = db.execute(
        "SELECT id FROM pending_words WHERE LOWER(word)=LOWER(?) AND ip_address=? "
        "AND submitted_at > datetime('now', '-5 minutes')",
        (word, ip)
    ).fetchone()
    if dup:
        return jsonify({"error": "Это слово уже было отправлено недавно"}), 429
    db.execute(
        "INSERT INTO pending_words (word, translation, example, locality, speaker, "
        "contributor, contact, direction, ip_address) VALUES (?,?,?,?,?,?,?,?,?)",
        (word, translation,
         str(data.get("example",     "")).strip(),
         str(data.get("locality",    "")).strip(),
         str(data.get("speaker",     "")).strip(),
         str(data.get("contributor", "")).strip(),
         str(data.get("contact",     "")).strip(),
         str(data.get("direction",   "ct_ru")),
         ip)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Слово отправлено на модерацию. Спасибо!"}), 201


# ── Административные ──────────────────────────────────────────────────────────

@app.route("/api/pending")
@require_admin
def get_pending():
    status = request.args.get("status", "pending")
    limit  = min(int(request.args.get("limit", 100)), 500)
    offset = int(request.args.get("offset", 0))
    db     = get_db()
    rows   = db.execute(
        "SELECT * FROM pending_words WHERE status=? ORDER BY submitted_at DESC LIMIT ? OFFSET ?",
        (status, limit, offset)
    ).fetchall()
    total = db.execute(
        "SELECT COUNT(*) FROM pending_words WHERE status=?", (status,)
    ).fetchone()[0]
    return jsonify({"total": total, "items": _rows(rows)})


@app.route("/api/approve/<int:word_id>", methods=["POST"])
@require_admin
def approve_word(word_id):
    db  = get_db()
    row = db.execute(
        "SELECT * FROM pending_words WHERE id=? AND status='pending'", (word_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Не найдено"}), 404
    if not DICT_PATH.exists():
        return jsonify({"error": "dictionary.db не загружен на сервер"}), 500
    r = dict(row)
    try:
        dc = sqlite3.connect(DICT_PATH)
        dc.row_factory = sqlite3.Row
        entry_lang = "ru" if r["direction"] == "ru_ct" else "ct"
        ex = dc.execute(
            "SELECT id FROM words WHERE crimean_tatar=? COLLATE NOCASE", (r["word"],)
        ).fetchone()
        if ex:
            dc.execute(
                "UPDATE words SET russian=?, definition_ct=?, tags=? WHERE id=?",
                (r["translation"], r["example"], r["locality"], ex["id"])
            )
        else:
            dc.execute(
                "INSERT INTO words (crimean_tatar, russian, definition_ct, tags, entry_lang) "
                "VALUES (?,?,?,?,?)",
                (r["word"], r["translation"], r["example"], r["locality"], entry_lang)
            )
        try:
            dc.execute("INSERT INTO words_fts(words_fts) VALUES('rebuild')")
        except Exception:
            pass
        dc.commit()
        dc.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    db.execute(
        "UPDATE pending_words SET status='approved', moderated_at=datetime('now') WHERE id=?",
        (word_id,)
    )
    last  = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v = last + 1
    db.execute(
        "INSERT INTO dict_versions (version, words_count, db_size_bytes, db_hash, release_note) "
        "VALUES (?, ?, ?, ?, ?)",
        (new_v, _word_count(), DICT_PATH.stat().st_size,
         _file_hash(DICT_PATH), f"Одобрено: {r['word']}")
    )
    db.commit()
    return jsonify({"ok": True, "new_version": new_v, "word": r["word"]})


@app.route("/api/reject/<int:word_id>", methods=["POST"])
@require_admin
def reject_word(word_id):
    db  = get_db()
    row = db.execute(
        "SELECT id FROM pending_words WHERE id=? AND status='pending'", (word_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Не найдено"}), 404
    data   = request.get_json(force=True, silent=True) or {}
    reason = str(data.get("reason", "")).strip()
    db.execute(
        "UPDATE pending_words SET status='rejected', reject_reason=?, moderated_at=datetime('now') WHERE id=?",
        (reason, word_id)
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/publish", methods=["POST"])
@require_admin
def publish_version():
    if not DICT_PATH.exists():
        return jsonify({"error": "dictionary.db не найден"}), 404
    db    = get_db()
    last  = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v = last + 1
    data  = request.get_json(force=True, silent=True) or {}
    note  = str(data.get("note", "Ручная публикация")).strip()
    db.execute(
        "INSERT INTO dict_versions (version, words_count, db_size_bytes, db_hash, release_note) "
        "VALUES (?, ?, ?, ?, ?)",
        (new_v, _word_count(), DICT_PATH.stat().st_size, _file_hash(DICT_PATH), note)
    )
    db.commit()
    return jsonify({"ok": True, "version": new_v, "words_count": _word_count()})


@app.route("/api/upload_dict", methods=["POST"])
@require_admin
def upload_dict():
    if "file" not in request.files:
        return jsonify({"error": "Файл не найден в запросе"}), 400
    f = request.files["file"]
    if not f.filename.endswith(".db"):
        return jsonify({"error": "Ожидается .db файл"}), 400
    tmp = BASE_DIR / "dictionary_tmp.db"
    f.save(tmp)
    try:
        tc = sqlite3.connect(tmp)
        wc = tc.execute("SELECT COUNT(*) FROM words").fetchone()[0]
        tc.close()
        if wc < 100:
            tmp.unlink()
            return jsonify({"error": "Слишком мало слов, файл подозрительный"}), 400
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return jsonify({"error": f"Невалидная БД: {e}"}), 400
    tmp.rename(DICT_PATH)
    db    = get_db()
    last  = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v = last + 1
    db.execute(
        "INSERT INTO dict_versions (version, words_count, db_size_bytes, db_hash, release_note) "
        "VALUES (?, ?, ?, ?, ?)",
        (new_v, wc, DICT_PATH.stat().st_size, _file_hash(DICT_PATH), "Первоначальная загрузка словаря")
    )
    db.commit()
    return jsonify({"ok": True, "words_count": wc, "version": new_v})


@app.route("/api/stats")
@require_admin
def get_stats():
    db       = get_db()
    pending  = db.execute("SELECT COUNT(*) FROM pending_words WHERE status='pending'").fetchone()[0]
    approved = db.execute("SELECT COUNT(*) FROM pending_words WHERE status='approved'").fetchone()[0]
    rejected = db.execute("SELECT COUNT(*) FROM pending_words WHERE status='rejected'").fetchone()[0]
    version  = db.execute(
        "SELECT version, words_count, released_at FROM dict_versions ORDER BY version DESC LIMIT 1"
    ).fetchone()
    recent   = db.execute(
        "SELECT word, translation, contributor, submitted_at, status "
        "FROM pending_words ORDER BY submitted_at DESC LIMIT 5"
    ).fetchall()
    return jsonify({
        "pending":      pending,
        "approved":     approved,
        "rejected":     rejected,
        "dict_version": _row(version),
        "db_ready":     DICT_PATH.exists(),
        "recent":       _rows(recent),
    })


# ── Пользователи ──────────────────────────────────────────────────────────────

@app.route("/api/users/login", methods=["POST"])
def user_login():
    data     = request.get_json(force=True, silent=True) or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    if not username or not password:
        return jsonify({"ok": False, "error": "Укажите логин и пароль"}), 400
    db  = get_db()
    row = db.execute(
        "SELECT * FROM server_users WHERE username=? AND password_hash=? AND is_active=1",
        (username, _hash_pw(password))
    ).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "Неверный логин или пароль"}), 401
    r = dict(row)
    db.execute("UPDATE server_users SET last_login=datetime('now') WHERE id=?", (r["id"],))
    db.commit()
    return jsonify({
        "ok":        True,
        "username":  r["username"],
        "role":      r["role"],
        "full_name": r["full_name"],
    })


@app.route("/api/users/list", methods=["GET"])
@require_admin
def user_list():
    db   = get_db()
    rows = db.execute(
        "SELECT id, username, role, full_name, is_active, created_at, last_login "
        "FROM server_users ORDER BY created_at DESC"
    ).fetchall()
    return jsonify({"ok": True, "users": _rows(rows)})


def _persist_users(db):
    """Читает server_users из SQLite и сохраняет в GitHub."""
    rows = db.execute(
        "SELECT username, password_hash, role, full_name, is_active FROM server_users"
    ).fetchall()
    _gh_save_users([dict(r) for r in rows])


@app.route("/api/users/create", methods=["POST"])
@require_admin
def user_create():
    data      = request.get_json(force=True, silent=True) or {}
    username  = data.get("username", "").strip()
    password  = data.get("password", "")
    role      = data.get("role", "editor")
    full_name = data.get("full_name", "").strip()
    if not username or not password:
        return jsonify({"ok": False, "error": "Укажите логин и пароль"}), 400
    if role not in ("admin", "editor"):
        role = "editor"
    db = get_db()
    try:
        db.execute(
            "INSERT INTO server_users (username, password_hash, role, full_name) "
            "VALUES (?, ?, ?, ?)",
            (username, _hash_pw(password), role, full_name)
        )
        db.commit()
        _persist_users(db)
        return jsonify({"ok": True, "message": f"Пользователь {username!r} создан"})
    except sqlite3.IntegrityError:
        return jsonify({"ok": False, "error": "Пользователь уже существует"}), 409


@app.route("/api/users/update", methods=["POST"])
@require_admin
def user_update():
    data    = request.get_json(force=True, silent=True) or {}
    user_id = data.get("user_id")
    db      = get_db()
    if "password" in data and data["password"]:
        db.execute("UPDATE server_users SET password_hash=? WHERE id=?",
                   (_hash_pw(data["password"]), user_id))
    if "role" in data:
        db.execute("UPDATE server_users SET role=? WHERE id=?", (data["role"], user_id))
    if "is_active" in data:
        db.execute("UPDATE server_users SET is_active=? WHERE id=?",
                   (1 if data["is_active"] else 0, user_id))
    if "full_name" in data:
        db.execute("UPDATE server_users SET full_name=? WHERE id=?",
                   (data["full_name"], user_id))
    db.commit()
    _persist_users(db)
    return jsonify({"ok": True})


@app.route("/api/users/delete", methods=["POST"])
@require_admin
def user_delete():
    data    = request.get_json(force=True, silent=True) or {}
    user_id = data.get("user_id")
    db      = get_db()
    db.execute("DELETE FROM server_users WHERE id=?", (user_id,))
    db.commit()
    _persist_users(db)
    return jsonify({"ok": True})


@app.route("/api/users/sync", methods=["POST"])
@require_admin
def sync_users():
    data  = request.get_json(force=True) or {}
    users = data.get("users", [])
    db    = get_db()
    db.execute("DELETE FROM remote_users WHERE username != 'admin'")
    added = 0
    for u in users:
        uname = u.get("username", "").strip()
        phash = u.get("password_hash", "").strip()
        role  = u.get("role", "editor")
        if uname and phash:
            db.execute(
                "INSERT OR REPLACE INTO remote_users (username, password_hash, role, synced_at) "
                "VALUES (?, ?, ?, datetime('now'))",
                (uname, phash, role)
            )
            added += 1
    db.commit()
    return jsonify({"ok": True, "synced": added})


@app.route("/api/users/check", methods=["POST"])
def check_user():
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip()
    phash = data.get("password_hash", "").strip()
    if not uname or not phash:
        return jsonify({"ok": False, "error": "Missing credentials"}), 400
    db  = get_db()
    row = db.execute(
        "SELECT role FROM remote_users WHERE username=? AND password_hash=?",
        (uname, phash)
    ).fetchone()
    if row:
        return jsonify({"ok": True, "role": dict(row)["role"]})
    return jsonify({"ok": False, "error": "Invalid credentials"}), 401


@app.route("/api/users", methods=["GET"])
@require_admin
def get_users():
    db   = get_db()
    rows = db.execute(
        "SELECT username, password_hash, role FROM remote_users ORDER BY username"
    ).fetchall()
    return jsonify({"users": _rows(rows)})


# ── Уведомления ───────────────────────────────────────────────────────────────

@app.route("/api/notifications/send", methods=["POST"])
@require_admin
def notifications_send():
    data    = request.get_json(force=True, silent=True) or {}
    title   = str(data.get("title",   "")).strip()
    message = str(data.get("message", "")).strip()
    target  = str(data.get("target",  "all")).strip()
    if not title or not message:
        return jsonify({"ok": False, "error": "Заголовок и текст обязательны"}), 400
    if target not in ("all", "editors", "admins"):
        target = "all"
    db = get_db()
    db.execute(
        "INSERT INTO notifications (title, message, target) VALUES (?, ?, ?)",
        (title, message, target)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Уведомление отправлено"})


@app.route("/api/notifications/list", methods=["GET"])
@require_admin
def notifications_list():
    db   = get_db()
    rows = db.execute(
        "SELECT id, title, message, target, created_at FROM notifications "
        "ORDER BY created_at DESC LIMIT 100"
    ).fetchall()
    return jsonify({"ok": True, "notifications": _rows(rows)})


@app.route("/api/notifications", methods=["GET"])
def get_notifications():
    role = request.args.get("role", "all")
    db   = get_db()
    if role == "admin":
        rows = db.execute(
            "SELECT id, title, message, target, created_at FROM notifications "
            "ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    elif role == "editor":
        rows = db.execute(
            "SELECT id, title, message, target, created_at FROM notifications "
            "WHERE target IN ('all', 'editors') ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, title, message, target, created_at FROM notifications "
            "WHERE target = 'all' ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
    return jsonify({"notifications": _rows(rows)})


# ── Обратная связь ───────────────────────────────────────────────────────────

@app.route("/api/feedback", methods=["POST"])
def submit_feedback():
    data      = request.get_json(force=True, silent=True) or {}
    message   = str(data.get("message",   "")).strip()
    contact   = str(data.get("contact",   "")).strip()
    device_id = str(data.get("device_id", "")).strip()
    if not message:
        return jsonify({"error": "Сообщение не может быть пустым"}), 400
    ip = request.remote_addr or ""
    db = get_db()
    db.execute(
        "INSERT INTO feedback (message, contact, device_id, ip_address) VALUES (?, ?, ?, ?)",
        (message, contact, device_id, ip)
    )
    db.commit()
    return jsonify({"ok": True}), 201


@app.route("/api/feedback_list")
@require_admin
def get_feedback_list():
    db   = get_db()
    rows = db.execute(
        "SELECT f.*, "
        "(SELECT COUNT(*) FROM feedback_replies r WHERE r.feedback_id=f.id) as reply_count "
        "FROM feedback f ORDER BY f.submitted_at DESC LIMIT 200"
    ).fetchall()
    unread = db.execute(
        "SELECT COUNT(*) FROM feedback WHERE is_read=0"
    ).fetchone()[0]
    return jsonify({"items": _rows(rows), "unread": unread})


@app.route("/api/feedback/<int:fid>/read", methods=["POST"])
@require_admin
def mark_feedback_read(fid):
    db = get_db()
    db.execute("UPDATE feedback SET is_read=1 WHERE id=?", (fid,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/feedback/<int:fid>/reply", methods=["POST"])
@require_admin
def reply_feedback(fid):
    data  = request.get_json(force=True, silent=True) or {}
    reply = str(data.get("reply", "")).strip()
    if not reply:
        return jsonify({"error": "Текст ответа не может быть пустым"}), 400
    db  = get_db()
    row = db.execute("SELECT device_id FROM feedback WHERE id=?", (fid,)).fetchone()
    if not row:
        return jsonify({"error": "Сообщение не найдено"}), 404
    device_id = dict(row)["device_id"]
    if not device_id:
        return jsonify({"error": "У пользователя нет device_id — ответ недоставим"}), 400
    db.execute(
        "INSERT INTO feedback_replies (feedback_id, device_id, reply) VALUES (?, ?, ?)",
        (fid, device_id, reply)
    )
    db.execute("UPDATE feedback SET is_read=1 WHERE id=?", (fid,))
    db.commit()
    return jsonify({"ok": True}), 201


@app.route("/api/feedback/replies/resend/<int:rid>", methods=["POST"])
@require_admin
def resend_reply(rid):
    """Сбросить флаг доставки — для повторной отправки ответа."""
    db = get_db()
    db.execute("UPDATE feedback_replies SET is_delivered=0 WHERE id=?", (rid,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/feedback/replies")
def get_replies():
    """Мобильный клиент запрашивает ответы по своему device_id."""
    device_id = request.args.get("device_id", "").strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    db   = get_db()
    rows = db.execute(
        "SELECT r.id, r.reply, r.created_at, f.message as original_message "
        "FROM feedback_replies r JOIN feedback f ON f.id=r.feedback_id "
        "WHERE r.device_id=? AND r.is_delivered=0 ORDER BY r.created_at",
        (device_id,)
    ).fetchall()
    if rows:
        ids = [dict(r)["id"] for r in rows]
        db.execute(
            f"UPDATE feedback_replies SET is_delivered=1 WHERE id IN ({','.join('?'*len(ids))})",
            ids
        )
        db.commit()
    return jsonify({"replies": _rows(rows)})


# ── Обновления приложения ─────────────────────────────────────────────────────

APP_VERSION = "1.5.0"
APP_BUILD   = 5


@app.route("/api/app_version")
def get_app_version():
    """Backward-compat endpoint for mobile client."""
    db  = get_db()
    row = db.execute(
        "SELECT version FROM app_releases ORDER BY released_at DESC LIMIT 1"
    ).fetchone()
    latest = dict(row)["version"] if row else APP_VERSION
    return jsonify({"version": latest, "build": APP_BUILD})


@app.route("/api/app/release", methods=["GET"])
def get_app_release():
    db  = get_db()
    row = db.execute(
        "SELECT version, download_url, changelog, released_at "
        "FROM app_releases ORDER BY released_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return jsonify({"version": None})
    return jsonify(dict(row))


@app.route("/api/app/release", methods=["POST"])
@require_admin
def set_app_release():
    data         = request.get_json(force=True, silent=True) or {}
    version      = str(data.get("version",      "")).strip()
    download_url = str(data.get("download_url", "")).strip()
    changelog    = str(data.get("changelog",    "")).strip()
    if not version or not download_url:
        return jsonify({"ok": False, "error": "Версия и ссылка обязательны"}), 400
    db = get_db()
    db.execute(
        "INSERT INTO app_releases (version, download_url, changelog) VALUES (?, ?, ?)",
        (version, download_url, changelog)
    )
    db.commit()
    return jsonify({"ok": True, "version": version})


# ── Фразеологизмы и словосочетания ───────────────────────────────────────────

def _extra_rows(db, table: str) -> list:
    rows = db.execute(
        f"SELECT crimean_tatar, russian, definition, example, category, "
        f"is_verified, entry_lang, created_at, updated_at FROM {table} "
        f"ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


@app.route("/api/extra/<table>", methods=["GET"])
def get_extra(table):
    if table not in ("phraseology", "collocations"):
        return jsonify({"error": "Not found"}), 404
    db = get_db()
    return jsonify({"ok": True, "rows": _extra_rows(db, table)})


@app.route("/api/extra/<table>/sync", methods=["POST"])
@require_admin
def sync_extra(table):
    if table not in ("phraseology", "collocations"):
        return jsonify({"error": "Not found"}), 404
    data = request.get_json(force=True, silent=True) or {}
    rows = data.get("rows", [])
    db   = get_db()
    db.execute(f"DELETE FROM {table}")
    for r in rows:
        db.execute(
            f"INSERT INTO {table} (crimean_tatar, russian, definition, example, "
            f"category, is_verified, entry_lang) VALUES (?,?,?,?,?,?,?)",
            (r.get("crimean_tatar",""), r.get("russian",""),
             r.get("definition",""), r.get("example",""),
             r.get("category",""), r.get("is_verified",0),
             r.get("entry_lang","ct"))
        )
    db.commit()
    _gh_save_extra(table, _extra_rows(db, table))
    return jsonify({"ok": True, "synced": len(rows)})


# ── Startup ───────────────────────────────────────────────────────────────────

with app.app_context():
    init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
