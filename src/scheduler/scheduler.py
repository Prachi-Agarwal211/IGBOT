from datetime import datetime, timedelta, time
import pytz
import random
from ..config import TIMEZONE
from .. import db

IST = pytz.timezone(TIMEZONE)

WINDOWS = [
    (time(11, 0), time(14, 0)),
    (time(18, 0), time(21, 0)),
]


def next_best_slot(now: datetime | None = None) -> datetime:
    now_ist = now.astimezone(IST) if now else datetime.now(IST)
    today = now_ist.date()

    # Check today windows first
    for start, end in WINDOWS:
        start_dt = IST.localize(datetime.combine(today, start))
        end_dt = IST.localize(datetime.combine(today, end))
        if now_ist <= end_dt:
            # if before start -> schedule at start; else next 5-min mark
            if now_ist <= start_dt:
                return start_dt
            else:
                candidate = now_ist + timedelta(minutes=5)
                return candidate.replace(second=0, microsecond=0)

    # Else schedule at next day's first window start
    tomorrow = today + timedelta(days=1)
    return IST.localize(datetime.combine(tomorrow, WINDOWS[0][0]))


# v2.1 daily planner with jitter and window weighting
def _weights_for_minute(minute_of_day: int) -> float:
    # Heavier weights in 11:00–13:00 and 18:00–22:00 IST
    hour = minute_of_day // 60
    if 11 <= hour < 13:
        return 2.0
    if 18 <= hour < 22:
        return 2.5
    if 0 <= hour < 5:
        return 1.2  # night owls
    return 1.0


def plan_randomized_slots_ist(day_ist: datetime, count: int, base_every_min: int, jitter_min: int) -> list[datetime]:
    """Return IST datetimes within the given day with base spacing and +/- jitter.
    day_ist: any datetime on the target day in IST.
    """
    day_start = IST.localize(datetime.combine(day_ist.date(), time(0, 0)))
    slots = []
    if count <= 0:
        return slots
    # Start evenly spaced, then apply jitter and probability weights acceptance
    interval = max(5, base_every_min)
    for i in range(count * 3):  # oversample to accommodate rejections
        minute = (i * interval) % (24 * 60)
        weight = _weights_for_minute(minute)
        if random.random() <= min(1.0, weight / 2.5):
            # create time and jitter
            base_dt = day_start + timedelta(minutes=minute)
            jitter = random.randint(-jitter_min, jitter_min)
            jittered = base_dt + timedelta(minutes=jitter)
            jittered = jittered.replace(second=0, microsecond=0)
            if 0 <= (jittered - day_start).total_seconds() < 86400:
                slots.append(jittered)
            if len(slots) >= count:
                break
    # Ensure sorted and unique
    slots = sorted(list(dict.fromkeys(slots)))
    return slots[:count]


def to_utc_iso_z(dt_ist: datetime) -> str:
    return dt_ist.astimezone(pytz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def plan_day(count_memes: int = 24, count_stories: int = 48, variant_picker=None):
    """Create jittered schedules for the current IST day.
    variant_picker: optional callable(meme_id)->variant_no for A/B; if None, pick 1.
    Note: This only creates schedule rows; it doesn't pick specific memes or stories.
    Use assigners below to bind items to slots.
    """
    now_ist = datetime.now(IST)
    meme_slots = plan_randomized_slots_ist(now_ist, count_memes, base_every_min=60, jitter_min=15)
    story_slots = plan_randomized_slots_ist(now_ist, count_stories, base_every_min=30, jitter_min=7)

    # Create placeholder schedules (without binding meme_id/story_id yet)
    for s in meme_slots:
        planned = s - timedelta(minutes=0)  # base already weighted; planned==slot before jitter if needed
        planned_iso = to_utc_iso_z(planned)
        scheduled_iso = to_utc_iso_z(s)
        db.create_schedule(kind='meme', planned_time_utc=planned_iso, jitter_sec=0, scheduled_time_utc=scheduled_iso,
                           meme_id=None, story_id=None, caption_variant_no=None, priority=0)

    for s in story_slots:
        planned_iso = to_utc_iso_z(s)
        scheduled_iso = to_utc_iso_z(s)
        db.create_schedule(kind='story', planned_time_utc=planned_iso, jitter_sec=0, scheduled_time_utc=scheduled_iso,
                           meme_id=None, story_id=None, caption_variant_no=None, priority=0)


def assign_memes_to_open_slots(meme_ids: list[int]):
    """Bind available meme_ids to the earliest unassigned meme schedules for today onward."""
    now_iso = to_utc_iso_z(datetime.now(IST))
    rows = db.fetch_due_schedules(now_iso=now_iso, kind=None, limit=None)  # get queued due; we need queued future too
    # Fetch queued future as well
    # Re-query directly for unassigned meme schedules regardless of time
    with db.get_conn() as conn:
        open_rows = conn.execute(
            "SELECT id FROM schedules WHERE kind='meme' AND status='queued' AND meme_id IS NULL ORDER BY scheduled_time_utc ASC"
        ).fetchall()
    for meme_id, row in zip(meme_ids, open_rows):
        sched_id = row[0]
        with db.get_conn() as conn:
            conn.execute("UPDATE schedules SET meme_id = ? WHERE id = ?", (meme_id, sched_id))
            conn.commit()


# Weekly planner: exact targets per day with jitter
def _times_to_datetimes(day_ist: datetime, times: list[time]) -> list[datetime]:
    return [IST.localize(datetime.combine(day_ist.date(), t)) for t in times]


def plan_week(days: int = 7, meme_jitter_min: int = 15, story_jitter_min: int = 7):
    """Create 7-day schedule using fixed target times and jitter.
    Memes/day: 12 targets; Stories/day: every 30m from 10:00 to 21:30 (24 slots).
    """
    base_meme_times = [
        time(7, 20), time(8, 10), time(8, 55),
        time(12, 10), time(12, 55), time(13, 40),
        time(19, 10), time(20, 0), time(20, 50), time(21, 40), time(22, 20),
        time(23, 45),
    ]
    # Stories every 30 minutes 10:00–21:30
    base_story_times = [time(h, m) for h in range(10, 22) for m in (0, 30)] + [time(21, 30)]

    start = datetime.now(IST)
    for d in range(days):
        day = start + timedelta(days=d)
        # Memes with jitter
        for t in base_meme_times:
            base_dt = IST.localize(datetime.combine(day.date(), t))
            jitter = random.randint(-meme_jitter_min, meme_jitter_min)
            slot = base_dt + timedelta(minutes=jitter)
            planned_iso = to_utc_iso_z(base_dt)
            scheduled_iso = to_utc_iso_z(slot)
            db.create_schedule(kind='meme', planned_time_utc=planned_iso, jitter_sec=jitter*60, scheduled_time_utc=scheduled_iso)
        # Stories with jitter
        for t in base_story_times:
            base_dt = IST.localize(datetime.combine(day.date(), t))
            jitter = random.randint(-story_jitter_min, story_jitter_min)
            slot = base_dt + timedelta(minutes=jitter)
            planned_iso = to_utc_iso_z(base_dt)
            scheduled_iso = to_utc_iso_z(slot)
            db.create_schedule(kind='story', planned_time_utc=planned_iso, jitter_sec=jitter*60, scheduled_time_utc=scheduled_iso)


# Simple random variant picker
def pick_variant_random(meme_id: int) -> int | None:
    variants = db.fetch_caption_variants(meme_id)
    if not variants:
        return None
    # variants rows: (variant_no, caption_text, hashtags)
    return random.choice(variants)[0]


def assign_memes_with_variants(meme_ids: list[int]):
    with db.get_conn() as conn:
        open_rows = conn.execute(
            "SELECT id FROM schedules WHERE kind='meme' AND status='queued' AND meme_id IS NULL ORDER BY scheduled_time_utc ASC"
        ).fetchall()
    for meme_id, row in zip(meme_ids, open_rows):
        sched_id = row[0]
        variant_no = pick_variant_random(meme_id)
        db.assign_schedule_meme(sched_id, meme_id, variant_no)


# Story payload generation per daypart
def generate_story_payloads_for_day(count: int = 48) -> list[tuple[str, str]]:
    """Return list of (story_type, payload_json_str)."""
    import json
    now_ist = datetime.now(IST)
    outputs = []
    types_cycle = [
        ("poll", {"question": "Sex on 1st date?", "options": ["Yes", "No"], "bias": "spicy"}),
        ("meme_war", {"image_a_url": "", "image_b_url": "", "question": "Which more savage?"}),
        ("confession", {"prompt": "Your worst date story?", "share_anonymously": True}),
        ("quiz", {"question": "Delhi traffic is:", "options": ["cardio", "purgatory", "karma"], "answer": None}),
        ("sticker_spam", {"stickers": ["😂", "🔥", "❤️"]}),
        ("screenshot_tweet", {"tweet_image_url": "", "caption_overlay": "Dekh lo bhai... desi reality"}),
    ]
    for i in range(count):
        t, payload = types_cycle[i % len(types_cycle)]
        # Daypart tweak
        hour = (now_ist.hour + (i // 2)) % 24
        if t == "poll" and 23 <= hour or hour <= 4:
            payload["question"] = "Late night plans? 😏"
            payload["options"] = ["Sleep", "DMs open"]
        outputs.append((t, json.dumps(payload)))
    return outputs


def create_and_assign_stories_to_open_slots(max_create: int = 48):
    payloads = generate_story_payloads_for_day(count=max_create)
    # Insert stories
    story_ids = [db.insert_story(t, p) for (t, p) in payloads]
    # Assign to open story schedules
    with db.get_conn() as conn:
        open_rows = conn.execute(
            "SELECT id FROM schedules WHERE kind='story' AND status='queued' AND story_id IS NULL ORDER BY scheduled_time_utc ASC"
        ).fetchall()
    for sid, row in zip(story_ids, open_rows):
        db.assign_schedule_story(row[0], sid)


# -------- Weekly detailed plan (export/ingest) --------
def _daily_exact_times():
    meme_times = [
        time(7, 20), time(8, 10), time(8, 55),
        time(12, 10), time(12, 55), time(13, 40),
        time(19, 10), time(20, 0), time(20, 50), time(21, 40), time(22, 20),
        time(23, 45),
    ]
    # reels: 3/day (approx 12:30, 19:45, 21:15)
    reel_times = [time(12, 30), time(19, 45), time(21, 15)]
    # stories: every 30 min 10:00–21:30
    story_times = [time(h, m) for h in range(10, 22) for m in (0, 30)] + [time(21, 30)]
    return meme_times, reel_times, story_times


def generate_week_plan(days: int = 7) -> list[dict]:
    """Return list of dict entries: {date, time, kind, category, format}.
    kind in ['meme','reel','story']. format for memes: 'static'|'carousel'.
    """
    categories_cycle = [
        "office/college", "bollywood", "cricket", "relationship", "evergreen desi", "regional", "money"
    ]
    meme_formats_cycle = ["static", "static", "carousel", "static", "carousel", "static"]
    meme_times, reel_times, story_times = _daily_exact_times()
    start = datetime.now(IST)
    plan = []
    for d in range(days):
        day = start + timedelta(days=d)
        cat_offset = d % len(categories_cycle)
        # memes
        for i, t in enumerate(meme_times):
            plan.append({
                "date": day.date().isoformat(),
                "time": f"{t.hour:02d}:{t.minute:02d}",
                "kind": "meme",
                "category": categories_cycle[(cat_offset + i) % len(categories_cycle)],
                "format": meme_formats_cycle[i % len(meme_formats_cycle)],
            })
        # reels
        for t in reel_times:
            plan.append({
                "date": day.date().isoformat(),
                "time": f"{t.hour:02d}:{t.minute:02d}",
                "kind": "reel",
                "category": categories_cycle[(cat_offset + 1) % len(categories_cycle)],
                "format": "reel",
            })
        # stories
        for t in story_times:
            plan.append({
                "date": day.date().isoformat(),
                "time": f"{t.hour:02d}:{t.minute:02d}",
                "kind": "story",
                "category": "engagement",
                "format": "story",
            })
    return plan


def export_week_plan_json(path: str, days: int = 7):
    import json, os
    plan = generate_week_plan(days=days)
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({"plan": plan}, f, ensure_ascii=False, indent=2)


def ingest_week_plan_json(path: str, meme_jitter_min: int = 15, story_jitter_min: int = 7, reel_jitter_min: int = 12):
    import json
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    entries = data.get("plan", [])
    for e in entries:
        day = datetime.strptime(e["date"], "%Y-%m-%d").date()
        hh, mm = map(int, e["time"].split(":"))
        base_dt = IST.localize(datetime.combine(day, time(hh, mm)))
        kind = e.get("kind", "meme")
        if kind == 'story':
            j = random.randint(-story_jitter_min, story_jitter_min)
        elif kind == 'reel':
            j = random.randint(-reel_jitter_min, reel_jitter_min)
        else:
            j = random.randint(-meme_jitter_min, meme_jitter_min)
        slot = base_dt + timedelta(minutes=j)
        planned_iso = to_utc_iso_z(base_dt)
        scheduled_iso = to_utc_iso_z(slot)
        # Allow 'reel' kind; posting engine may treat it later
        db.create_schedule(kind=kind, planned_time_utc=planned_iso, jitter_sec=j*60, scheduled_time_utc=scheduled_iso)
