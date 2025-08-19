import sqlite3
from contextlib import contextmanager
from typing import Optional, List, Tuple
from datetime import datetime
from .config import DB_PATH

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS memes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                title TEXT,
                image_url TEXT,
                ocr_text TEXT,
                caption TEXT,
                hashtags TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                scheduled_time TEXT,
                published_time TEXT,
                error TEXT
            )
            """
        )
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_memes_source_sourceid ON memes(source, source_id)")
        # v2: caption variants
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS captions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meme_id INTEGER NOT NULL,
                variant_no INTEGER NOT NULL,
                caption_text TEXT NOT NULL,
                hashtags TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(meme_id, variant_no),
                FOREIGN KEY(meme_id) REFERENCES memes(id) ON DELETE CASCADE
            )
            """
        )
        # Backfill: try add ocr_text if older DB lacks it
        try:
            c.execute("ALTER TABLE memes ADD COLUMN ocr_text TEXT")
        except sqlite3.OperationalError:
            pass

        # v2.1: unified schedules table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL, -- 'meme' | 'story'
                meme_id INTEGER,
                story_id INTEGER,
                caption_variant_no INTEGER,
                planned_time_utc TEXT NOT NULL,
                jitter_sec INTEGER NOT NULL DEFAULT 0,
                scheduled_time_utc TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'instagram',
                status TEXT NOT NULL DEFAULT 'queued', -- queued|posted|failed|skipped
                priority INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                UNIQUE(kind, meme_id, scheduled_time_utc),
                FOREIGN KEY(meme_id) REFERENCES memes(id) ON DELETE CASCADE
            )
            """
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_sched_due ON schedules(status, scheduled_time_utc)")

        # posts table to record published items
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                platform_post_id TEXT,
                posted_at_utc TEXT,
                status TEXT NOT NULL,
                error TEXT,
                FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE
            )
            """
        )

        # analytics table to store fetched insights per post
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                metric TEXT NOT NULL,
                value REAL,
                captured_at_utc TEXT NOT NULL,
                UNIQUE(post_id, metric, captured_at_utc),
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            )
            """
        )

        # stories placeholder table (for future story payloads)
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS stories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                story_type TEXT NOT NULL, -- poll|quiz|screenshot|tag_template|image
                payload_json TEXT,
                status TEXT NOT NULL DEFAULT 'new'
            )
            """
        )
        # hashtag pools for rotation
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS hashtag_pools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                tags_csv TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(name)
            )
            """
        )
        # audio pools for reels planning
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS audio_pools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                items_json TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(name)
            )
            """
        )
        # v2.2: carousel support
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS carousels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                caption TEXT,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS carousel_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                carousel_id INTEGER NOT NULL,
                image_url TEXT NOT NULL,
                position INTEGER NOT NULL,
                FOREIGN KEY(carousel_id) REFERENCES carousels(id) ON DELETE CASCADE
            )
            """
        )
        # schedules: add carousel_id if missing
        try:
            c.execute("ALTER TABLE schedules ADD COLUMN carousel_id INTEGER")
        except sqlite3.OperationalError:
            pass
        conn.commit()


def insert_meme(source: str, source_id: str, title: str, image_url: str) -> bool:
    with get_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO memes (source, source_id, title, image_url) VALUES (?, ?, ?, ?)",
                (source, source_id, title, image_url),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def create_meme_returning_id(source: str, source_id: str, title: str, image_url: str) -> int:
    """Create a meme row and return its id. If already exists, return the existing id."""
    with get_conn() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO memes (source, source_id, title, image_url) VALUES (?, ?, ?, ?)",
                (source, source_id, title, image_url),
            )
            conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT id FROM memes WHERE source = ? AND source_id = ?",
                (source, source_id),
            ).fetchone()
            if not row:
                raise
            return row[0]


def fetch_memes_by_status(status: str, limit: Optional[int] = None) -> List[Tuple]:
    with get_conn() as conn:
        q = "SELECT id, source, source_id, title, image_url, caption, hashtags, status, scheduled_time FROM memes WHERE status = ? ORDER BY id DESC"
        if limit:
            q += " LIMIT ?"
            rows = conn.execute(q, (status, limit)).fetchall()
        else:
            rows = conn.execute(q, (status,)).fetchall()
        return rows


def fetch_new_memes_with_ocr(limit: int = 50) -> List[Tuple]:
    """Return list of (id, source, source_id, title, image_url, ocr_text) for status 'new'"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, source, source_id, title, image_url, ocr_text FROM memes WHERE status = 'new' ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return rows


# stories helpers
def insert_story(story_type: str, payload_json: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO stories (story_type, payload_json, status) VALUES (?, ?, 'ready')",
            (story_type, payload_json),
        )
        conn.commit()
        return cur.lastrowid


def fetch_ready_stories(limit: Optional[int] = None) -> List[Tuple[int, str, str]]:
    """Return (id, story_type, payload_json) for ready stories."""
    with get_conn() as conn:
        q = "SELECT id, story_type, payload_json FROM stories WHERE status='ready' ORDER BY id ASC"
        if limit:
            q += " LIMIT ?"
            rows = conn.execute(q, (limit,)).fetchall()
        else:
            rows = conn.execute(q).fetchall()
        return rows


# schedule querying/updating
def fetch_unassigned_schedules(kind: str, limit: Optional[int] = None) -> List[Tuple]:
    with get_conn() as conn:
        column = 'meme_id' if kind in ('meme', 'reel') else ('story_id' if kind == 'story' else 'carousel_id')
        q = "SELECT id FROM schedules WHERE kind = ? AND status = 'queued' AND {} IS NULL ORDER BY scheduled_time_utc ASC".format(
            column
        )
        if limit:
            q += " LIMIT ?"
            rows = conn.execute(q, (kind, limit)).fetchall()
        else:
            rows = conn.execute(q, (kind,)).fetchall()
        return rows


def assign_schedule_meme(schedule_id: int, meme_id: int, variant_no: Optional[int]):
    with get_conn() as conn:
        conn.execute(
            "UPDATE schedules SET meme_id = ?, caption_variant_no = ? WHERE id = ?",
            (meme_id, variant_no, schedule_id),
        )
        conn.commit()


def assign_schedule_story(schedule_id: int, story_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE schedules SET story_id = ? WHERE id = ?",
            (story_id, schedule_id),
        )
        conn.commit()


def assign_schedule_carousel(schedule_id: int, carousel_id: int):
    with get_conn() as conn:
        conn.execute(
            "UPDATE schedules SET carousel_id = ? WHERE id = ?",
            (carousel_id, schedule_id),
        )
        conn.commit()


# hashtag pool helpers
def upsert_hashtag_pool(name: str, tags_csv: str, active: int = 1):
    with get_conn() as conn:
        conn.execute("INSERT INTO hashtag_pools(name, tags_csv, active) VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET tags_csv=excluded.tags_csv, active=excluded.active", (name, tags_csv, active))
        conn.commit()


def get_hashtag_pool(name: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT tags_csv FROM hashtag_pools WHERE name = ? AND active = 1",
            (name,),
        ).fetchone()
        return row[0] if row else None


# audio pool helpers
def upsert_audio_pool(name: str, items_json: str, active: int = 1):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO audio_pools(name, items_json, active) VALUES (?, ?, ?) ON CONFLICT(name) DO UPDATE SET items_json=excluded.items_json, active=excluded.active",
            (name, items_json, active),
        )
        conn.commit()


def get_audio_pool(name: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT items_json FROM audio_pools WHERE name = ? AND active = 1",
            (name,),
        ).fetchone()
        return row[0] if row else None


def get_meme(meme_id: int) -> Optional[Tuple]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, image_url, caption, hashtags FROM memes WHERE id = ?",
            (meme_id,),
        ).fetchone()
        return row


# Carousel helpers
def create_carousel_from_memes(meme_ids: List[int], caption: Optional[str]) -> int:
    """Create a carousel from meme image URLs. Returns carousel_id."""
    if not meme_ids or len(meme_ids) < 2:
        raise ValueError("Carousel requires at least 2 meme ids")
    with get_conn() as conn:
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        cur = conn.execute(
            "INSERT INTO carousels(caption, created_at_utc) VALUES(?, ?)",
            (caption or "", now),
        )
        carousel_id = cur.lastrowid
        # fetch urls preserving given order
        pos = 0
        for mid in meme_ids:
            row = conn.execute("SELECT image_url FROM memes WHERE id = ?", (mid,)).fetchone()
            if not row or not row[0]:
                continue
            pos += 1
            conn.execute(
                "INSERT INTO carousel_items(carousel_id, image_url, position) VALUES(?, ?, ?)",
                (carousel_id, row[0], pos),
            )
        conn.commit()
        if pos < 2:
            raise RuntimeError("Not enough valid images to build carousel")
        return carousel_id


def get_carousel(carousel_id: int) -> Tuple[str, List[str]]:
    """Return (caption, image_urls ordered)."""
    with get_conn() as conn:
        cap_row = conn.execute("SELECT caption FROM carousels WHERE id = ?", (carousel_id,)).fetchone()
        if not cap_row:
            raise RuntimeError("Carousel not found")
        urls = [r[0] for r in conn.execute(
            "SELECT image_url FROM carousel_items WHERE carousel_id = ? ORDER BY position ASC",
            (carousel_id,),
        ).fetchall()]
        return cap_row[0] or "", urls


def create_carousel_from_urls(image_urls: List[str], caption: Optional[str]) -> int:
    """Create a carousel given a list of image URLs (2-10). Returns carousel_id."""
    urls = [u for u in (image_urls or []) if u]
    if len(urls) < 2:
        raise ValueError("Carousel requires at least 2 images")
    urls = urls[:10]
    with get_conn() as conn:
        now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        cur = conn.execute(
            "INSERT INTO carousels(caption, created_at_utc) VALUES(?, ?)",
            (caption or "", now),
        )
        carousel_id = cur.lastrowid
        for pos, u in enumerate(urls, start=1):
            conn.execute(
                "INSERT INTO carousel_items(carousel_id, image_url, position) VALUES(?, ?, ?)",
                (carousel_id, u, pos),
            )
        conn.commit()
        return carousel_id


def get_caption_variant(meme_id: int, variant_no: int) -> Optional[Tuple[str, str]]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT caption_text, hashtags FROM captions WHERE meme_id = ? AND variant_no = ? AND active = 1",
            (meme_id, variant_no),
        ).fetchone()
        return row


# v2.1 schedules helpers
def create_schedule(kind: str, planned_time_utc: str, jitter_sec: int, scheduled_time_utc: str,
                    meme_id: Optional[int] = None, story_id: Optional[int] = None,
                    caption_variant_no: Optional[int] = None, priority: int = 0):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO schedules(kind, meme_id, story_id, caption_variant_no, planned_time_utc, jitter_sec, scheduled_time_utc, platform, status, priority)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'instagram', 'queued', ?)
            """,
            (kind, meme_id, story_id, caption_variant_no, planned_time_utc, jitter_sec, scheduled_time_utc, priority),
        )
        conn.commit()


def create_schedule_returning_id(kind: str, planned_time_utc: str, jitter_sec: int, scheduled_time_utc: str,
                                 meme_id: Optional[int] = None, story_id: Optional[int] = None,
                                 caption_variant_no: Optional[int] = None, priority: int = 0) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO schedules(kind, meme_id, story_id, caption_variant_no, planned_time_utc, jitter_sec, scheduled_time_utc, platform, status, priority)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'instagram', 'queued', ?)
            """,
            (kind, meme_id, story_id, caption_variant_no, planned_time_utc, jitter_sec, scheduled_time_utc, priority),
        )
        conn.commit()
        return cur.lastrowid


def fetch_due_schedules(now_iso: str, kind: Optional[str] = None, limit: Optional[int] = None) -> List[Tuple]:
    with get_conn() as conn:
        base = "SELECT id, kind, meme_id, story_id, carousel_id, caption_variant_no, scheduled_time_utc FROM schedules WHERE status = 'queued' AND scheduled_time_utc <= ?"
        params = [now_iso]
        if kind:
            base += " AND kind = ?"
            params.append(kind)
        base += " ORDER BY scheduled_time_utc ASC"
        if limit:
            base += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(base, tuple(params)).fetchall()
        return rows


def mark_schedule_posted(schedule_id: int, posted_iso: str, platform_post_id: str = ""):
    with get_conn() as conn:
        conn.execute("UPDATE schedules SET status = 'posted', error = NULL WHERE id = ?", (schedule_id,))
        conn.execute(
            "INSERT INTO posts(schedule_id, platform_post_id, posted_at_utc, status) VALUES (?, ?, ?, 'posted')",
            (schedule_id, platform_post_id, posted_iso),
        )
        conn.commit()


def mark_schedule_failed(schedule_id: int, error: str):
    with get_conn() as conn:
        conn.execute("UPDATE schedules SET status = 'failed', error = ? WHERE id = ?", (error, schedule_id))
        conn.execute(
            "INSERT INTO posts(schedule_id, status, error) VALUES (?, 'failed', ?)",
            (schedule_id, error),
        )
        conn.commit()


def fetch_posts_since(iso_utc: str) -> List[Tuple]:
    """Return posts with platform ids since a UTC ISO time. Rows: (id, schedule_id, platform_post_id, posted_at_utc)"""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, schedule_id, platform_post_id, posted_at_utc FROM posts WHERE posted_at_utc >= ? AND status = 'posted' AND platform_post_id IS NOT NULL AND platform_post_id != '' ORDER BY posted_at_utc ASC",
            (iso_utc,),
        ).fetchall()
        return rows


def insert_analytics(post_id: int, metric: str, value: float, captured_at_utc: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO analytics(post_id, metric, value, captured_at_utc) VALUES (?, ?, ?, ?)",
            (post_id, metric, value, captured_at_utc),
        )
        conn.commit()


def update_caption_hashtags(meme_id: int, caption: str, hashtags: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE memes SET caption = ?, hashtags = ?, status = 'ready' WHERE id = ?",
            (caption, hashtags, meme_id),
        )
        conn.commit()


def schedule_meme(meme_id: int, when_iso: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE memes SET scheduled_time = ?, status = 'queued' WHERE id = ?",
            (when_iso, meme_id),
        )
        conn.commit()


def fetch_due_memes(now_iso: str, limit: Optional[int] = None) -> List[Tuple]:
    with get_conn() as conn:
        q = "SELECT id, image_url, caption, hashtags FROM memes WHERE status = 'queued' AND scheduled_time <= ? ORDER BY scheduled_time ASC"
        if limit:
            q += " LIMIT ?"
            rows = conn.execute(q, (now_iso, limit)).fetchall()
        else:
            rows = conn.execute(q, (now_iso,)).fetchall()
        return rows


def mark_published(meme_id: int, published_iso: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE memes SET status = 'posted', published_time = ?, error = NULL WHERE id = ?",
            (published_iso, meme_id),
        )
        conn.commit()


def mark_failed(meme_id: int, error: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE memes SET status = 'failed', error = ? WHERE id = ?",
            (error, meme_id),
        )
        conn.commit()


# v2 helpers for caption variants
def insert_caption_variants(meme_id: int, variants: List[tuple]):
    """variants: List[(variant_no:int, caption_text:str, hashtags:str)]"""
    with get_conn() as conn:
        for variant_no, caption_text, hashtags in variants:
            conn.execute(
                "INSERT OR REPLACE INTO captions (meme_id, variant_no, caption_text, hashtags, active) VALUES (?, ?, ?, ?, 1)",
                (meme_id, variant_no, caption_text, hashtags),
            )
        conn.commit()


def fetch_caption_variants(meme_id: int) -> List[Tuple]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT variant_no, caption_text, hashtags FROM captions WHERE meme_id = ? AND active = 1 ORDER BY variant_no ASC",
            (meme_id,),
        ).fetchall()
        return rows


# v2: OCR helpers
def set_ocr_text(meme_id: int, text: str):
    with get_conn() as conn:
        conn.execute("UPDATE memes SET ocr_text = ? WHERE id = ?", (text, meme_id))
        conn.commit()


def fetch_memes_needing_ocr(limit: int = 50) -> List[Tuple]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, image_url FROM memes WHERE (ocr_text IS NULL OR ocr_text = '') ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return rows
