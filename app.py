"""
Lügat — Центральный сервер краудсорсинга
"""
import os
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import Flask, request, jsonify, send_file, g
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR    = Path(__file__).parent
DB_PATH     = BASE_DIR / "lugat_server.db"
DICT_PATH   = BASE_DIR / "dictionary.db"
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "lugat_admin_2024")


SERVER_SALT = "lugat_server_users_2024"

def _hash_pw(password: str) -> str:
    return hashlib.sha256(f"{SERVER_SALT}{password}".encode()).hexdigest()

def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-Admin-Token", "")
        if token != ADMIN_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

def get_db():
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS pending_words (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            word            TEXT NOT NULL,
            translation     TEXT NOT NULL,
            example         TEXT DEFAULT '',
            locality        TEXT DEFAULT '',
            speaker         TEXT DEFAULT '',
            contributor     TEXT DEFAULT '',
            contact         TEXT DEFAULT '',
            direction       TEXT DEFAULT 'ct_ru',
            status          TEXT DEFAULT 'pending',
            reject_reason   TEXT DEFAULT '',
            submitted_at    TEXT DEFAULT (datetime('now')),
            moderated_at    TEXT DEFAULT NULL,
            ip_address      TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS moderation_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id     INTEGER,
            action      TEXT,
            admin_note  TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS dict_versions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            version         INTEGER NOT NULL UNIQUE,
            words_count     INTEGER DEFAULT 0,
            db_size_bytes   INTEGER DEFAULT 0,
            db_hash         TEXT DEFAULT '',
            released_at     TEXT DEFAULT (datetime('now')),
            release_note    TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_words(status);
        CREATE TABLE IF NOT EXISTS server_users (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            username     TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role         TEXT DEFAULT 'editor',
            full_name    TEXT DEFAULT '',
            is_active    INTEGER DEFAULT 1,
            created_at   TEXT DEFAULT (datetime('now')),
            last_login   TEXT DEFAULT NULL
        );
    """)
    # Если dictionary.db есть — создаём начальную версию
    row = conn.execute("SELECT COUNT(*) FROM dict_versions").fetchone()[0]
    if row == 0 and DICT_PATH.exists():
        size = DICT_PATH.stat().st_size
        hsh  = _file_hash(DICT_PATH)
        wc   = _word_count()
        conn.execute(
            "INSERT INTO dict_versions (version, words_count, db_size_bytes, db_hash, release_note) "
            "VALUES (1,?,?,?,'Начальная версия')", (wc, size, hsh)
        )
    conn.commit()
    conn.close()


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


# ── Публичные ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"name": "Lügat API", "version": "1.0", "status": "running"})


@app.route("/api/version")
def get_version():
    db  = get_db()
    row = db.execute(
        "SELECT version, words_count, db_size_bytes, db_hash, released_at "
        "FROM dict_versions ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if not row:
        return jsonify({"version": 0, "words_count": 0, "db_ready": False})
    return jsonify({
        "version":     row["version"],
        "words_count": row["words_count"],
        "db_size":     row["db_size_bytes"],
        "db_hash":     row["db_hash"],
        "released_at": row["released_at"],
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
    if client_version >= row["version"] and client_hash == row["db_hash"]:
        return jsonify({"up_to_date": True, "version": row["version"]})
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
        "SELECT id FROM pending_words WHERE word=? COLLATE NOCASE AND ip_address=? "
        "AND submitted_at > datetime('now','-5 minutes')", (word, ip)
    ).fetchone()
    if dup:
        return jsonify({"error": "Это слово уже было отправлено недавно"}), 429
    db.execute(
        "INSERT INTO pending_words (word,translation,example,locality,speaker,"
        "contributor,contact,direction,ip_address) VALUES (?,?,?,?,?,?,?,?,?)",
        (word, translation,
         str(data.get("example","")).strip(),
         str(data.get("locality","")).strip(),
         str(data.get("speaker","")).strip(),
         str(data.get("contributor","")).strip(),
         str(data.get("contact","")).strip(),
         str(data.get("direction","ct_ru")),
         ip)
    )
    db.commit()
    return jsonify({"ok": True, "message": "Слово отправлено на модерацию. Спасибо!"}), 201


# ── Административные ─────────────────────────────────────────────────────────

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
    return jsonify({"total": total, "items": [dict(r) for r in rows]})


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
    try:
        dc = sqlite3.connect(DICT_PATH)
        entry_lang = "ru" if row["direction"] == "ru_ct" else "ct"
        ex = dc.execute(
            "SELECT id FROM words WHERE crimean_tatar=? COLLATE NOCASE", (row["word"],)
        ).fetchone()
        if ex:
            dc.execute(
                "UPDATE words SET russian=?,definition_ct=?,tags=? WHERE id=?",
                (row["translation"], row["example"], row["locality"], ex["id"])
            )
        else:
            dc.execute(
                "INSERT INTO words (crimean_tatar,russian,definition_ct,tags,entry_lang) "
                "VALUES (?,?,?,?,?)",
                (row["word"], row["translation"], row["example"], row["locality"], entry_lang)
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
        "UPDATE pending_words SET status='approved',moderated_at=datetime('now') WHERE id=?",
        (word_id,)
    )
    last = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v = last + 1
    db.execute(
        "INSERT INTO dict_versions (version,words_count,db_size_bytes,db_hash,release_note) "
        "VALUES (?,?,?,?,?)",
        (new_v, _word_count(), DICT_PATH.stat().st_size,
         _file_hash(DICT_PATH), f"Одобрено: {row['word']}")
    )
    db.commit()
    return jsonify({"ok": True, "new_version": new_v, "word": row["word"]})


@app.route("/api/reject/<int:word_id>", methods=["POST"])
@require_admin
def reject_word(word_id):
    db     = get_db()
    row    = db.execute(
        "SELECT id FROM pending_words WHERE id=? AND status='pending'", (word_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Не найдено"}), 404
    data   = request.get_json(force=True, silent=True) or {}
    reason = str(data.get("reason","")).strip()
    db.execute(
        "UPDATE pending_words SET status='rejected',reject_reason=?,"
        "moderated_at=datetime('now') WHERE id=?", (reason, word_id)
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/publish", methods=["POST"])
@require_admin
def publish_version():
    if not DICT_PATH.exists():
        return jsonify({"error": "dictionary.db не найден"}), 404
    db      = get_db()
    last    = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v   = last + 1
    data    = request.get_json(force=True, silent=True) or {}
    note    = str(data.get("note","Ручная публикация")).strip()
    db.execute(
        "INSERT INTO dict_versions (version,words_count,db_size_bytes,db_hash,release_note) "
        "VALUES (?,?,?,?,?)",
        (new_v, _word_count(), DICT_PATH.stat().st_size, _file_hash(DICT_PATH), note)
    )
    db.commit()
    return jsonify({"ok": True, "version": new_v, "words_count": _word_count()})


@app.route("/api/upload_dict", methods=["POST"])
@require_admin
def upload_dict():
    """Загружает dictionary.db на сервер (для первоначальной настройки)."""
    if "file" not in request.files:
        return jsonify({"error": "Файл не найден в запросе"}), 400
    f = request.files["file"]
    if not f.filename.endswith(".db"):
        return jsonify({"error": "Ожидается .db файл"}), 400
    tmp = BASE_DIR / "dictionary_tmp.db"
    f.save(tmp)
    # Проверяем что это валидная SQLite БД со словами
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
    # Создаём версию
    db    = get_db()
    last  = db.execute("SELECT COALESCE(MAX(version),0) FROM dict_versions").fetchone()[0]
    new_v = last + 1
    db.execute(
        "INSERT INTO dict_versions (version,words_count,db_size_bytes,db_hash,release_note) "
        "VALUES (?,?,?,?,?)",
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
        "SELECT version,words_count,released_at FROM dict_versions ORDER BY version DESC LIMIT 1"
    ).fetchone()
    recent   = db.execute(
        "SELECT word,translation,contributor,submitted_at,status "
        "FROM pending_words ORDER BY submitted_at DESC LIMIT 5"
    ).fetchall()
    return jsonify({
        "pending":      pending,
        "approved":     approved,
        "rejected":     rejected,
        "dict_version": dict(version) if version else None,
        "db_ready":     DICT_PATH.exists(),
        "recent":       [dict(r) for r in recent],
    })


# ── Управление пользователями ────────────────────────────────────────────────

@app.route("/api/users/login", methods=["POST"])
def user_login():
    """Авторизация редактора/администратора через сервер."""
    data = request.get_json(force=True, silent=True) or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    if not username or not password:
        return jsonify({"ok": False, "error": "Укажите логин и пароль"}), 400

    db = get_db()
    row = db.execute(
        "SELECT * FROM server_users WHERE username=? AND password_hash=? AND is_active=1",
        (username, _hash_pw(password))
    ).fetchone()

    if not row:
        return jsonify({"ok": False, "error": "Неверный логин или пароль"}), 401

    # Обновляем last_login
    db.execute(
        "UPDATE server_users SET last_login=datetime('now') WHERE id=?",
        (row["id"],)
    )
    db.commit()

    return jsonify({
        "ok": True,
        "username": row["username"],
        "role": row["role"],
        "full_name": row["full_name"],
    })


@app.route("/api/users/list", methods=["GET"])
@require_admin
def user_list():
    """Список всех пользователей (только для admin)."""
    db = get_db()
    rows = db.execute(
        "SELECT id, username, role, full_name, is_active, created_at, last_login "
        "FROM server_users ORDER BY created_at DESC"
    ).fetchall()
    return jsonify({"ok": True, "users": [dict(r) for r in rows]})


@app.route("/api/users/create", methods=["POST"])
@require_admin
def user_create():
    """Создать нового пользователя."""
    data = request.get_json(force=True, silent=True) or {}
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
        return jsonify({"ok": True, "message": f"Пользователь {username!r} создан"})
    except Exception as e:
        return jsonify({"ok": False, "error": f"Пользователь уже существует: {e}"}), 409


@app.route("/api/users/update", methods=["POST"])
@require_admin
def user_update():
    """Изменить роль, пароль или активность пользователя."""
    data    = request.get_json(force=True, silent=True) or {}
    user_id = data.get("user_id")
    db      = get_db()

    if "password" in data and data["password"]:
        db.execute(
            "UPDATE server_users SET password_hash=? WHERE id=?",
            (_hash_pw(data["password"]), user_id)
        )
    if "role" in data:
        db.execute("UPDATE server_users SET role=? WHERE id=?", (data["role"], user_id))
    if "is_active" in data:
        db.execute("UPDATE server_users SET is_active=? WHERE id=?",
                   (1 if data["is_active"] else 0, user_id))
    if "full_name" in data:
        db.execute("UPDATE server_users SET full_name=? WHERE id=?",
                   (data["full_name"], user_id))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/users/delete", methods=["POST"])
@require_admin
def user_delete():
    """Удалить пользователя."""
    data    = request.get_json(force=True, silent=True) or {}
    user_id = data.get("user_id")
    db      = get_db()
    db.execute("DELETE FROM server_users WHERE id=?", (user_id,))
    db.commit()
    return jsonify({"ok": True})


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


# ── Управление пользователями (удалённая синхронизация) ───────────────────────

@app.route("/api/users", methods=["GET"])
@require_admin
def get_users():
    """Получить список пользователей (для синхронизации клиентов)."""
    db = get_db()
    rows = db.execute(
        "SELECT username, password_hash, role FROM remote_users ORDER BY username"
    ).fetchall()
    return jsonify({"users": [dict(r) for r in rows]})


@app.route("/api/users/sync", methods=["POST"])
@require_admin
def sync_users():
    """
    Принять список пользователей от admin-клиента.
    Полностью заменяет список на сервере.
    Body: {"users": [{"username": ..., "password_hash": ..., "role": ...}]}
    """
    data  = request.get_json(force=True) or {}
    users = data.get("users", [])

    db = get_db()
    # Создаём таблицу если нет
    db.execute("""
        CREATE TABLE IF NOT EXISTS remote_users (
            username      TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            role          TEXT DEFAULT 'editor',
            synced_at     TEXT DEFAULT (datetime('now'))
        )
    """)
    # Удаляем всех кроме admin
    db.execute("DELETE FROM remote_users WHERE username != 'admin'")
    # Вставляем/обновляем
    added = 0
    for u in users:
        uname = u.get("username", "").strip()
        phash = u.get("password_hash", "").strip()
        role  = u.get("role", "editor")
        if uname and phash:
            db.execute("""
                INSERT OR REPLACE INTO remote_users (username, password_hash, role, synced_at)
                VALUES (?, ?, ?, datetime('now'))
            """, (uname, phash, role))
            added += 1
    db.commit()
    return jsonify({"ok": True, "synced": added})


@app.route("/api/users/check", methods=["POST"])
def check_user():
    """
    Проверить логин/пароль пользователя (для удалённого входа).
    Не требует admin-токен — нужен только username+password_hash.
    Body: {"username": ..., "password_hash": ...}
    """
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip()
    phash = data.get("password_hash", "").strip()

    if not uname or not phash:
        return jsonify({"ok": False, "error": "Missing credentials"}), 400

    db = get_db()
    # Создаём таблицу если нет
    try:
        db.execute("CREATE TABLE IF NOT EXISTS remote_users (username TEXT PRIMARY KEY, password_hash TEXT, role TEXT DEFAULT 'editor', synced_at TEXT)")
        row = db.execute(
            "SELECT role FROM remote_users WHERE username=? AND password_hash=?",
            (uname, phash)
        ).fetchone()
    except Exception:
        row = None

    if row:
        return jsonify({"ok": True, "role": row["role"]})
    return jsonify({"ok": False, "error": "Invalid credentials"}), 401
