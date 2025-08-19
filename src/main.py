import argparse
from datetime import datetime
from typing import List
from datetime import timedelta
import pytz
import time
import json

from . import db
from .config import DEFAULT_SUBREDDITS
from .scraper.reddit_scraper import scrape_subreddits
from .scraper.twitter_scraper import scrape_twitter_images
from .scraper.youtube_scraper import download_videos
from .processor.captioner import generate_caption_hashtags
from .processor.captioner import generate_caption_variants
from .processor.reels import batch_process_directory
from .processor.carousel_builder import process_directory as process_carousel_dir
from .analyzer.trends import TrendAnalyzer
from .analyzer.audio import TrendingAudioAnalyzer
from .engagement.agent import EngagementAgent
from .scheduler.scheduler import (
    next_best_slot,
    plan_day,
    plan_reels_day,
    assign_memes_to_open_slots,
    assign_memes_with_variants,
    create_and_assign_stories_to_open_slots,
    plan_week,
    export_week_plan_json,
    ingest_week_plan_json,
)
from .publisher.instagram_client import InstagramClient
from .publisher.uploader import upload_directory
from .analyzer.trends import TrendAnalyzer
from .analyzer.ocr import extract_text_from_url
from .creative.templates import export_caption_frameworks_json, export_story_prompts_json


def _rotate_hashtags(schedule_id: int) -> str:
    """Build a shuffled hashtag string from rotating pools. Limit to 25 tags."""
    pools = ["trending", "evergreen", "niche", "regional"]
    picks = []
    for i, name in enumerate(pools):
        csv = db.get_hashtag_pool(name) or ""
        tags = [t.strip() for t in csv.split(",") if t.strip()]
        if not tags:
            continue
        # rotate by schedule_id for natural shuffle
        offset = (schedule_id + i * 3) % len(tags)
        rotated = tags[offset:] + tags[:offset]
        picks.extend(rotated[:8 if name != "regional" else 5])
    # de-dupe, keep order, cut to 25
    seen = set()
    unique = []
    for t in picks:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return " ".join(f"#{t}" for t in unique[:25])


def cmd_scrape(subreddits: List[str], limit: int):
    db.init_db()
    inserted = scrape_subreddits(subreddits, limit)
    print(f"Inserted {inserted} new memes from Reddit.")


def cmd_generate(pool: str | None = None):
    db.init_db()
    items = db.fetch_memes_by_status("new", limit=100)
    print(f"Generating captions for {len(items)} memes...")
    for (meme_id, source, source_id, title, image_url, *_rest) in items:
        caption, hashtags = generate_caption_hashtags(title, source, pool_name=pool)
        db.update_caption_hashtags(meme_id, caption, hashtags)
        print(f"Generated for id={meme_id}")


def cmd_twitter_scrape(query: str, limit: int):
    db.init_db()
    inserted = scrape_twitter_images(query=query, max_results=limit)
    print(f"Inserted {inserted} new memes from Twitter.")


def cmd_ocr(limit: int):
    db.init_db()
    items = db.fetch_memes_needing_ocr(limit=limit)
    print(f"Running OCR for {len(items)} memes...")
    for meme_id, image_url in items:
        try:
            text = extract_text_from_url(image_url)
            db.set_ocr_text(meme_id, text)
            print(f"OCR id={meme_id}: {len(text)} chars")
        except Exception as e:
            print(f"OCR failed id={meme_id}: {e}")


def cmd_generate_variants(variant_count: int, limit: int, pool: str | None = None):
    db.init_db()
    items = db.fetch_new_memes_with_ocr(limit=limit)
    print(f"Generating up to {variant_count} variants for {len(items)} memes...")
    for (meme_id, source, source_id, title, image_url, ocr_text) in items:
        context = (title or "")
        if ocr_text:
            context = f"{context}\nText on meme:\n{ocr_text}" if context else f"Text on meme:\n{ocr_text}"
        try:
            variants = generate_caption_variants(context_text=context, category=None, variant_count=variant_count, pool_name=pool)
            # Store variants and set first one as the meme's current caption/hashtags
            numbered = [(i + 1, cap, tags) for i, (cap, tags) in enumerate(variants)]
            db.insert_caption_variants(meme_id, numbered)
            first_cap, first_tags = variants[0]
            db.update_caption_hashtags(meme_id, first_cap, first_tags)
            print(f"Variants stored id={meme_id}: {len(variants)}")
        except Exception as e:
            print(f"Variant gen failed id={meme_id}: {e}")


def cmd_schedule(per_posts: int):
    db.init_db()
    ready = db.fetch_memes_by_status("ready", limit=per_posts)
    print(f"Scheduling {len(ready)} posts...")
    # Start from next best slot in IST, then convert and store as UTC
    when_ist = next_best_slot()
    when_utc = when_ist.astimezone(pytz.UTC)
    for (meme_id, *_rest) in ready:
        iso = when_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        db.schedule_meme(meme_id, iso)
        print(f"Queued id={meme_id} at {iso}")
        # stagger by 40 minutes in UTC
        when_utc = when_utc + timedelta(minutes=40)


def cmd_post_due(max_posts: int | None = None):
    db.init_db()
    now_iso = datetime.utcnow().isoformat() + "Z"
    due = db.fetch_due_memes(now_iso, limit=max_posts)
    if not due:
        print("No posts due.")
        return
    print(f"Posting {len(due)} memes...")
    ig = InstagramClient()
    for (meme_id, image_url, caption, hashtags) in due:
        try:
            # Publish with clean caption; move hashtags to first comment for better reach
            post_id = ig.post_photo(image_url, caption or "")
            if hashtags:
                try:
                    ig.create_comment(post_id, hashtags)
                except Exception as ce:
                    # Don't fail the post if comment fails
                    print(f"Warn: first comment failed id={meme_id}: {ce}")
            db.mark_published(meme_id, now_iso)
            print(f"Posted id={meme_id} -> IG media {post_id}")
        except Exception as e:
            db.mark_failed(meme_id, str(e))
            print(f"Failed id={meme_id}: {e}")


def cmd_plan_day(memes: int, stories: int, reels: int):
    db.init_db()
    plan_day(count_memes=memes, count_stories=stories)
    if reels > 0:
        plan_reels_day(count_reels=reels)
    print(f"Planned {memes} meme slots, {stories} story slots, and {reels} reel slots with jitter in schedules table.")


def cmd_post_due_all(max_items: int | None = None):
    """Process unified schedules (memes + stories). Stories are stubbed for now."""
    db.init_db()
    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = db.fetch_due_schedules(now_iso=now_iso, kind=None, limit=max_items)
    if not rows:
        print("No schedules due.")
        return
    ig = InstagramClient()
    for (schedule_id, kind, meme_id, story_id, carousel_id, caption_variant_no, _when) in rows:
        try:
            if kind == 'meme' and meme_id:
                m = db.get_meme(meme_id)
                if not m:
                    raise RuntimeError(f"Meme {meme_id} missing")
                _, image_url, base_caption, base_tags = m
                if caption_variant_no:
                    cv = db.get_caption_variant(meme_id, caption_variant_no)
                    if cv:
                        base_caption, base_tags = cv
                # Hashtag rotation at post time
                rotated = _rotate_hashtags(schedule_id)
                tags_combined = " ".join([t for t in [base_tags or "", rotated] if t]).strip()
                # Publish with clean caption; push tags to first comment
                caption_only = (base_caption or "").strip()
                media_id = ig.post_photo(image_url, caption_only)
                if tags_combined:
                    try:
                        ig.create_comment(media_id, tags_combined)
                    except Exception as ce:
                        print(f"Warn: first comment failed schedule={schedule_id}: {ce}")
                db.mark_schedule_posted(schedule_id, now_iso, platform_post_id=media_id)
                print(f"Posted schedule={schedule_id} meme_id={meme_id} -> {media_id}")
            elif kind == 'story':
                # Placeholder: Stories posting not implemented yet
                db.mark_schedule_posted(schedule_id, now_iso, platform_post_id="")
                print(f"Marked story schedule={schedule_id} as posted (placeholder)")
            elif kind == 'carousel' and carousel_id:
                # Fetch carousel assets and caption, then publish and add hashtags as first comment
                caption_c, image_urls = db.get_carousel(carousel_id)
                # Build rotated hashtags pool and move to first comment
                rotated = _rotate_hashtags(schedule_id)
                caption_only = (caption_c or "").strip()
                media_id = ig.post_carousel(image_urls, caption_only)
                if rotated:
                    try:
                        ig.create_comment(media_id, rotated)
                    except Exception as ce:
                        print(f"Warn: first comment failed (carousel) schedule={schedule_id}: {ce}")
                db.mark_schedule_posted(schedule_id, now_iso, platform_post_id=media_id)
                print(f"Posted carousel schedule={schedule_id} carousel_id={carousel_id} -> {media_id}")
            elif kind == 'reel' and meme_id:
                # For reels, we expect meme.image_url to be a video URL; if not, skip for now
                m = db.get_meme(meme_id)
                if not m:
                    raise RuntimeError(f"Meme {meme_id} missing for reel")
                _, media_url, base_caption, base_tags = m
                rotated = _rotate_hashtags(schedule_id)
                tags_combined = " ".join([t for t in [base_tags or "", rotated] if t]).strip()
                try:
                    # Publish with clean caption and add hashtags as first comment
                    media_id = ig.post_reel(media_url, (base_caption or "").strip())
                    if tags_combined:
                        try:
                            ig.create_comment(media_id, tags_combined)
                        except Exception as ce:
                            print(f"Warn: first comment failed (reel) schedule={schedule_id}: {ce}")
                    db.mark_schedule_posted(schedule_id, now_iso, platform_post_id=media_id)
                    print(f"Posted reel schedule={schedule_id} meme_id={meme_id} -> {media_id}")
                except Exception as re:
                    raise RuntimeError(f"Reel publish failed: {re}")
            else:
                print(f"Unknown kind or missing ids for schedule={schedule_id}, skipping")
        except Exception as e:
            db.mark_schedule_failed(schedule_id, str(e))
            print(f"Failed schedule={schedule_id}: {e}")


def cmd_assign_memes(limit: int):
    db.init_db()
    rows = db.fetch_memes_by_status("ready", limit=limit)
    ids = [r[0] for r in rows]
    if not ids:
        print("No ready memes to assign.")
        return
    assign_memes_to_open_slots(ids)
    print(f"Assigned {len(ids)} memes to earliest open meme schedule slots.")


def cmd_assign_memes_variants(limit: int):
    db.init_db()
    rows = db.fetch_memes_by_status("ready", limit=limit)
    ids = [r[0] for r in rows]
    if not ids:
        print("No ready memes to assign.")
        return
    assign_memes_with_variants(ids)
    print(f"Assigned {len(ids)} memes with random variants to schedule slots.")


def cmd_seed_hashtags():
    db.init_db()
    # Pools from strategy; users can edit later
    db.upsert_hashtag_pool(
        "trending",
        ",".join([
            "indianmemes","desihumor","hindimemes","reelkarofeelkaro","funnyindia","memesindia","reelsindia","memeindia","dankmemesindia","trendingreels"
        ]),
    )
    db.upsert_hashtag_pool(
        "evergreen",
        ",".join([
            "relatable","desivibes","desiculture","memepage","lolindia","indiangags","dailyfunny","memesofinstagram","chillvibes","pettyhumor"
        ]),
    )
    db.upsert_hashtag_pool(
        "niche",
        ",".join([
            "engineerlife","collegememes","hostellifeindia","delhimetro","bangaloretraffic","mumbaidreams","startupmemes","itlife","officehumor","chaiaddict"
        ]),
    )
    db.upsert_hashtag_pool(
        "regional",
        ",".join([
            "dilseindian","delhivibes","bangalorelife","mumbaivibes","cricketlover","bollywoodmemes"
        ]),
    )
    print("Seeded hashtag pools: trending, evergreen, niche, regional")


def cmd_gen_assign_stories(max_create: int):
    db.init_db()
    create_and_assign_stories_to_open_slots(max_create=max_create)
    print(f"Generated and assigned up to {max_create} stories into open schedule slots.")


def cmd_plan_week(days: int, meme_jitter: int, story_jitter: int, reel_jitter: int):
    db.init_db()
    # Use provided reel jitter for weekly reels
    plan_week(days=days, meme_jitter_min=meme_jitter, story_jitter_min=story_jitter, reel_jitter_min=reel_jitter)
    print(f"Planned {days} day(s) with fixed windows and jitter (reel jitter={reel_jitter}m).")


def cmd_export_week(json_path: str, days: int):
    export_week_plan_json(json_path, days=days)
    print(f"Exported {days}-day plan to {json_path}")


def cmd_ingest_week(json_path: str, meme_jitter: int, story_jitter: int, reel_jitter: int):
    db.init_db()
    ingest_week_plan_json(json_path, meme_jitter_min=meme_jitter, story_jitter_min=story_jitter, reel_jitter_min=reel_jitter)
    print(f"Ingested plan from {json_path} and created schedules with jitter.")


def cmd_export_story_prompts(out_path: str):
    export_story_prompts_json(out_path)
    print(f"Exported story prompts to {out_path}")


def cmd_export_caption_frameworks(out_path: str):
    export_caption_frameworks_json(out_path)
    print(f"Exported caption frameworks to {out_path}")


def cmd_create_carousel(meme_ids: list[int], caption: str | None):
    db.init_db()
    cid = db.create_carousel_from_memes(meme_ids, caption)
    print(f"Created carousel id={cid} with {len(meme_ids)} items (invalid image URLs are skipped)")


def cmd_schedule_carousel(carousel_id: int, when_utc_iso: str, priority: int = 0):
    """Create a schedule entry for a carousel at a specific UTC ISO time and assign it."""
    db.init_db()
    # planned_time_utc equals scheduled_time_utc, jitter = 0
    db.create_schedule(kind="carousel", planned_time_utc=when_utc_iso, jitter_sec=0, scheduled_time_utc=when_utc_iso, priority=priority)
    # Find the newly created open carousel slot and assign
    rows = db.fetch_unassigned_schedules(kind="carousel", limit=1)
    if not rows:
        print("No open carousel schedule slot found to assign.")
        return
    sched_id = rows[0][0]
    db.assign_schedule_carousel(sched_id, carousel_id)
    print(f"Scheduled carousel_id={carousel_id} at {when_utc_iso} as schedule_id={sched_id}")


def cmd_youtube_scrape(query: str, max_videos: int, out_dir: str):
    rows = download_videos(query=query, max_videos=max_videos, out_dir=out_dir)
    print(f"Downloaded {len(rows)} videos for query='{query}' into '{out_dir}'.")


def cmd_trends(subreddits: List[str], twitter_query: str, out_path: str | None):
    ta = TrendAnalyzer()
    data = ta.aggregate(subreddits=subreddits, twitter_query=twitter_query)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved trends to {out_path}")
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_reels_process(in_dir: str, out_dir: str, max_duration: int, fps: int, vbitrate: str, abitrate: str):
    rows = batch_process_directory(
        in_dir=in_dir,
        out_dir=out_dir,
        max_duration=max_duration,
        target_fps=fps,
        video_bitrate=vbitrate,
        audio_bitrate=abitrate,
    )
    print(f"Processed {len(rows)} reels into '{out_dir}'.")


def cmd_reels_upload(in_dir: str, prefix: str, out_json: str | None):
    urls = upload_directory(in_dir=in_dir, prefix=prefix)
    if out_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({"uploaded": urls}, f, ensure_ascii=False, indent=2)
        print(f"Uploaded {len(urls)} files. URLs saved to {out_json}")
    else:
        print(json.dumps({"uploaded": urls}, ensure_ascii=False, indent=2))


def cmd_reels_schedule(urls_json: str, start_utc: str, every_min: int, priority: int):
    """Schedule reels from a JSON file: {"uploaded": ["https://...mp4", ...]}"""
    db.init_db()
    with open(urls_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    urls = data.get("uploaded") or []
    if not urls:
        print("No URLs found in JSON (expected key 'uploaded').")
        return
    try:
        t0 = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
    except Exception:
        raise ValueError("Invalid --start-utc; expected UTC ISO e.g. 2025-08-19T14:30:00Z")
    scheduled = []
    for i, url in enumerate(urls):
        # derive a source_id from filename to ensure idempotency
        source_id = url.split("/")[-1]
        meme_id = db.create_meme_returning_id(source="reels-upload", source_id=source_id, title=source_id, image_url=url)
        when = (t0 + timedelta(minutes=i * every_min)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        sched_id = db.create_schedule_returning_id(kind="reel", planned_time_utc=when, jitter_sec=0, scheduled_time_utc=when, meme_id=meme_id, priority=priority)
        scheduled.append({"schedule_id": sched_id, "meme_id": meme_id, "url": url, "when": when})
    print(json.dumps({"scheduled": scheduled}, ensure_ascii=False, indent=2))


def cmd_build_carousel(in_dir: str, out_dir: str, s3_prefix: str, caption: str | None):
    """Process images to 1080x1350, upload to S3, and create a carousel record."""
    db.init_db()
    # 1) Process
    outputs = process_carousel_dir(in_dir=in_dir, out_dir=out_dir)
    if len(outputs) < 2:
        print("Need at least 2 processed images to create a carousel.")
        return
    # 2) Upload
    urls = upload_directory(in_dir=out_dir, prefix=s3_prefix)
    if len(urls) < 2:
        print("Upload produced fewer than 2 URLs; aborting.")
        return
    # 3) DB record
    cid = db.create_carousel_from_urls(urls, caption)
    print(json.dumps({"carousel_id": cid, "images": urls}, ensure_ascii=False, indent=2))


def cmd_trending_audio(path: str, top_n: int, out_path: str | None, to_pool: str | None = None, csv_out: str | None = None):
    ta = TrendingAudioAnalyzer()
    rows = ta.top_from_file(path, top_n=top_n)
    if to_pool:
        # persist in DB as an audio pool
        db.init_db()
        db.upsert_audio_pool(to_pool, json.dumps(rows, ensure_ascii=False))
    if csv_out:
        import csv as _csv
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            w.writerow(["audio", "count"]) 
            for r in rows:
                w.writerow([r.get("audio", ""), r.get("count", 0)])
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"trending_audio": rows}, f, ensure_ascii=False, indent=2)
        print(f"Saved top {len(rows)} audio entries to {out_path}")
    else:
        print(json.dumps({"trending_audio": rows}, ensure_ascii=False, indent=2))


def cmd_engage(since_utc: str, max_replies: int):
    agent = EngagementAgent()
    count = agent.run(since_utc_iso=since_utc, max_replies=max_replies)
    print(f"Engagement replies made: {count}")


def cmd_reels_pipeline(in_dir: str, out_dir: str, max_duration: int, fps: int, vbitrate: str, abitrate: str,
                       prefix: str, start_utc: str, every_min: int, priority: int, out_json: str | None, pool: str | None = None):
    # 1) Process
    rows = batch_process_directory(
        in_dir=in_dir,
        out_dir=out_dir,
        max_duration=max_duration,
        target_fps=fps,
        video_bitrate=vbitrate,
        audio_bitrate=abitrate,
    )
    # 2) Upload
    urls = upload_directory(in_dir=out_dir, prefix=prefix)
    # 3) Schedule
    try:
        t0 = datetime.fromisoformat(start_utc.replace("Z", "+00:00"))
    except Exception:
        raise ValueError("Invalid --start-utc; expected UTC ISO e.g. 2025-08-19T14:30:00Z")
    db.init_db()
    scheduled = []
    for i, url in enumerate(urls):
        source_id = url.split("/")[-1]
        meme_id = db.create_meme_returning_id(source="reels-upload", source_id=source_id, title=source_id, image_url=url)
        # Optional caption enrichment using hashtag pool
        try:
            cap, tags = generate_caption_hashtags(source_id, "reels-upload", pool_name=pool)
            db.update_caption_hashtags(meme_id, cap, tags)
        except Exception as ge:
            print(f"Warn: caption generation failed for {source_id}: {ge}")
        when = (t0 + timedelta(minutes=i * every_min)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        sched_id = db.create_schedule_returning_id(kind="reel", planned_time_utc=when, jitter_sec=0, scheduled_time_utc=when, meme_id=meme_id, priority=priority)
        scheduled.append({"schedule_id": sched_id, "meme_id": meme_id, "url": url, "when": when})
    payload = {
        "processed": len(rows),
        "uploaded": len(urls),
        "scheduled": scheduled,
    }
    if out_json:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Pipeline complete. Wrote summary to {out_json}")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_build_hashtag_pool(name: str, subreddits: list[str], twitter_query: str, top_n_trends: int, max_tags: int = 50):
    """Aggregate trends and upsert a hashtag pool in DB.
    Pool stores comma-separated tags (without #)."""
    db.init_db()
    ta = TrendAnalyzer()
    agg = ta.aggregate(subreddits=subreddits, twitter_query=twitter_query, top_n_trends=top_n_trends)
    tags: list[str] = []
    # Twitter hashtags
    for h in agg.get('twitter_hashtags') or []:
        tag = str(h.get('hashtag', '')).strip().lower()
        if tag:
            tags.append(tag)
    # Google trends -> convert phrases to hashtags
    for kw in agg.get('google_trends_in') or []:
        s = str(kw).strip().lower()
        if not s:
            continue
        cleaned = ''.join(ch for ch in s if ch.isalnum() or ch.isspace()).strip().replace(' ', '')
        if cleaned:
            tags.append(cleaned)
    # Reddit titles -> extract simple keywords (alnum words length>=4)
    for item in agg.get('reddit_hot') or []:
        title = str(item.get('title', '')).lower()
        word = ''.join(ch if ch.isalnum() or ch.isspace() else ' ' for ch in title)
        for w in word.split():
            if len(w) >= 5:
                tags.append(w)
    # Dedup and cap
    seen = set()
    final: list[str] = []
    for t in tags:
        t = t.lstrip('#')
        if not t or t in seen:
            continue
        seen.add(t)
        final.append(t)
        if len(final) >= max_tags:
            break
    csv = ','.join(final)
    db.upsert_hashtag_pool(name, csv, active=1)
    print(json.dumps({"pool": name, "count": len(final), "tags": final}, ensure_ascii=False, indent=2))

def cmd_auto_run(setup: bool, loop_sleep_sec: int, scrape_limit: int, twitter_query: str, twitter_limit: int,
                 variant_count: int, assign_limit: int, story_create: int):
    """Run the full pipeline in a loop. Use --setup once to seed hashtags.
    This will continuously:
      - scrape (Reddit + Twitter)
      - OCR
      - generate variants
      - assign memes and stories
      - post due items
      - sleep and repeat
    """
    db.init_db()
    if setup:
        cmd_seed_hashtags()
        print("Setup complete: hashtag pools seeded.")

    print("Starting auto-run loop. Press Ctrl+C to stop.")
    while True:
        try:
            cmd_scrape(DEFAULT_SUBREDDITS, scrape_limit)
            cmd_twitter_scrape(twitter_query, twitter_limit)
            cmd_ocr(100)
            cmd_generate_variants(variant_count, 100)
            cmd_assign_memes_variants(assign_limit)
            cmd_gen_assign_stories(story_create)
            cmd_post_due_all(30)
        except Exception as e:
            print(f"Auto-run iteration error: {e}")
        time.sleep(loop_sleep_sec)


def cmd_fetch_insights(since_utc_iso: str):
    db.init_db()
    rows = db.fetch_memes_by_status("ready", limit=100)
    ids = [r[0] for r in rows]
    if not ids:
        print("No ready memes to assign.")
        return
    ig = InstagramClient()
    for meme_id in ids:
        try:
            post_id = ig.post_photo(meme_id, "Test caption")
            db.mark_published(meme_id, since_utc_iso)
            print(f"Posted id={meme_id} -> IG media {post_id}")
        except Exception as e:
            db.mark_failed(meme_id, str(e))
            print(f"Failed id={meme_id}: {e}")


def main():
    p = argparse.ArgumentParser(description="IG Meme Content Farm")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_scrape = sub.add_parser("scrape", help="Scrape memes from Reddit")
    p_scrape.add_argument("--subreddits", nargs="*", default=DEFAULT_SUBREDDITS)
    p_scrape.add_argument("--limit", type=int, default=30)

    p_gen = sub.add_parser("generate", help="Generate captions/hashtags via Gemini")
    p_gen.add_argument("--pool", type=str, default=None, help="Optional hashtag pool name to enrich hashtags")

    p_tw = sub.add_parser("twitter-scrape", help="Scrape memes from Twitter via API v2")
    p_tw.add_argument("--query", type=str, default="(meme OR memes) (india OR indian) lang:en -is:retweet has:images")
    p_tw.add_argument("--limit", type=int, default=50)

    p_ocr = sub.add_parser("ocr", help="Extract text from meme images using Tesseract")
    p_ocr.add_argument("--limit", type=int, default=50)

    p_gv = sub.add_parser("generate-variants", help="Generate 3-5 caption variants per meme and store them")
    p_gv.add_argument("--variant-count", type=int, default=3)
    p_gv.add_argument("--limit", type=int, default=50)
    p_gv.add_argument("--pool", type=str, default=None, help="Optional hashtag pool name to enrich hashtags in variants")

    p_sched = sub.add_parser("schedule", help="Schedule ready posts")
    p_sched.add_argument("--per-posts", type=int, default=3)

    p_post = sub.add_parser("post-due", help="Publish due posts to Instagram")
    p_post.add_argument("--max-posts", type=int, default=None)

    p_plan = sub.add_parser("plan-day", help="Create randomized daily schedule entries for memes, reels, and stories")
    p_plan.add_argument("--memes", type=int, default=24)
    p_plan.add_argument("--stories", type=int, default=48)
    p_plan.add_argument("--reels", type=int, default=3)

    p_all = sub.add_parser("post-due-all", help="Process unified schedules (memes + stories)")
    p_all.add_argument("--max-items", type=int, default=None)

    p_assign = sub.add_parser("assign-memes", help="Bind ready memes to earliest open meme schedule slots")
    p_assign.add_argument("--limit", type=int, default=50)

    p_assignv = sub.add_parser("assign-memes-variants", help="Assign ready memes to schedule slots with random caption variants")
    p_assignv.add_argument("--limit", type=int, default=50)

    p_hashtags = sub.add_parser("seed-hashtags", help="Seed default hashtag pools into DB")

    p_stories = sub.add_parser("gen-assign-stories", help="Generate default story payloads and assign to open schedule slots")
    p_stories.add_argument("--max-create", type=int, default=48)

    p_week = sub.add_parser("plan-week", help="Create schedules for a full week using fixed time windows plus jitter")
    p_week.add_argument("--days", type=int, default=7)
    p_week.add_argument("--meme-jitter", type=int, default=15)
    p_week.add_argument("--story-jitter", type=int, default=7)
    p_week.add_argument("--reel-jitter", type=int, default=12)

    p_export = sub.add_parser("export-week-plan", help="Export weekly plan JSON for review/editing")
    p_export.add_argument("--out", type=str, default="week_plan.json")
    p_export.add_argument("--days", type=int, default=7)

    p_ingest = sub.add_parser("ingest-week-plan", help="Ingest edited weekly plan JSON and create schedules with jitter")
    p_ingest.add_argument("--path", type=str, default="week_plan.json")
    p_ingest.add_argument("--meme-jitter", type=int, default=15)
    p_ingest.add_argument("--story-jitter", type=int, default=7)
    p_ingest.add_argument("--reel-jitter", type=int, default=12)

    p_exp_prompts = sub.add_parser("export-story-prompts", help="Export 60+ story prompts JSON for planning")
    p_exp_prompts.add_argument("--out", type=str, default="story_prompts.json")

    p_exp_caps = sub.add_parser("export-caption-frameworks", help="Export caption frameworks JSON")
    p_exp_caps.add_argument("--out", type=str, default="caption_frameworks.json")

    p_fetch_ins = sub.add_parser("fetch-insights", help="Fetch IG insights for posts since a UTC ISO time")
    p_fetch_ins.add_argument("--since", type=str, required=True, help="UTC ISO time e.g. 2025-08-01T00:00:00Z")

    p_yt = sub.add_parser("youtube-scrape", help="Download short videos from YouTube using yt-dlp")
    p_yt.add_argument("--query", type=str, required=True, help="Search query e.g. 'funny indian meme'")
    p_yt.add_argument("--max-videos", type=int, default=10)
    p_yt.add_argument("--out-dir", type=str, default="content farm/videos")

    # Carousel management
    p_cc = sub.add_parser("create-carousel", help="Create a carousel from meme IDs")
    p_cc.add_argument("--meme-ids", type=int, nargs="+", required=True, help="Meme IDs in order")
    p_cc.add_argument("--caption", type=str, default=None)

    # Carousel builder: process local images and upload
    p_bc = sub.add_parser("build-carousel", help="Process images to 1080x1350, upload to S3, and create carousel")
    p_bc.add_argument("--in-dir", type=str, required=True, help="Input directory with images (2-10)")
    p_bc.add_argument("--out-dir", type=str, default="content farm/carousel")
    p_bc.add_argument("--prefix", type=str, default="carousels/")
    p_bc.add_argument("--caption", type=str, default=None)

    # Build hashtag pool from trends
    p_hp = sub.add_parser("build-hashtag-pool", help="Aggregate trends and create/update a hashtag pool")
    p_hp.add_argument("--name", type=str, required=True, help="Pool name, e.g. 'india-trending'")
    p_hp.add_argument("--subreddits", nargs="*", default=DEFAULT_SUBREDDITS)
    p_hp.add_argument("--twitter-query", type=str, default="meme OR funny lang:en -is:retweet")
    p_hp.add_argument("--top-n-trends", type=int, default=20)
    p_hp.add_argument("--max-tags", type=int, default=50)

    p_sc = sub.add_parser("schedule-carousel", help="Schedule an existing carousel at a UTC ISO time")
    p_sc.add_argument("--carousel-id", type=int, required=True)
    p_sc.add_argument("--when", type=str, required=True, help="UTC ISO e.g. 2025-08-19T18:00:00Z")
    p_sc.add_argument("--priority", type=int, default=0)

    p_tr = sub.add_parser("trends", help="Fetch aggregated trends (Google Trends IN, Reddit hot, Twitter hashtags)")
    p_tr.add_argument("--subreddits", nargs="*", default=DEFAULT_SUBREDDITS)
    p_tr.add_argument("--twitter-query", type=str, default="meme OR funny lang:en -is:retweet")
    p_tr.add_argument("--out", type=str, default=None, help="Optional JSON output path")

    # Trending audio analyzer (MVP: file-based aggregation)
    p_ta = sub.add_parser("trending-audio", help="Aggregate top audio IDs/links from a file (json/csv/txt)")
    p_ta.add_argument("--file", type=str, required=True, help="Path to json/csv/txt with audio entries")
    p_ta.add_argument("--top", type=int, default=25)
    p_ta.add_argument("--out", type=str, default=None, help="Optional JSON output path")
    p_ta.add_argument("--to-pool", type=str, default=None, help="Optional audio pool name to save top items to DB")
    p_ta.add_argument("--csv-out", type=str, default=None, help="Optional CSV export path with columns audio,count")

    # Build audio pool directly
    p_bap = sub.add_parser("build-audio-pool", help="Create/update an audio pool from a file of audios")
    p_bap.add_argument("--name", type=str, required=True, help="Audio pool name, e.g. 'india-audio'")
    p_bap.add_argument("--file", type=str, required=True, help="Path to json/csv/txt with audio entries")
    p_bap.add_argument("--top", type=int, default=50)

    # Engagement agent (stub)
    p_eng = sub.add_parser("engage", help="Engagement stub: validate since time and return 0 replies")
    p_eng.add_argument("--since", type=str, required=True, help="UTC ISO time e.g. 2025-08-01T00:00:00Z")
    p_eng.add_argument("--max-replies", type=int, default=10)

    # Reels one-shot pipeline
    p_rpl = sub.add_parser("reels-pipeline", help="Process -> upload -> schedule reels in one command")
    p_rpl.add_argument("--in-dir", type=str, default="content farm/videos")
    p_rpl.add_argument("--out-dir", type=str, default="content farm/reels")
    p_rpl.add_argument("--max-duration", type=int, default=58)
    p_rpl.add_argument("--fps", type=int, default=30)
    p_rpl.add_argument("--vbitrate", type=str, default="5M")
    p_rpl.add_argument("--abitrate", type=str, default="128k")
    p_rpl.add_argument("--prefix", type=str, default="reels/")
    p_rpl.add_argument("--start-utc", type=str, required=True, help="UTC ISO start time e.g. 2025-08-19T14:30:00Z")
    p_rpl.add_argument("--every-min", type=int, default=45)
    p_rpl.add_argument("--priority", type=int, default=0)
    p_rpl.add_argument("--out-json", type=str, default=None)
    p_rpl.add_argument("--pool", type=str, default=None, help="Optional hashtag pool name to enrich reel captions")

    p_auto = sub.add_parser("auto-run", help="Run full pipeline in a loop (one command daemon)")
    p_auto.add_argument("--setup", action="store_true", help="Seed hashtags once before starting loop")
    p_auto.add_argument("--sleep", type=int, default=300, help="Sleep seconds between iterations (default 300)")
    p_auto.add_argument("--scrape-limit", type=int, default=60)
    p_auto.add_argument("--twitter-query", type=str, default="meme OR funny -nsfw")
    p_auto.add_argument("--twitter-limit", type=int, default=60)
    p_auto.add_argument("--variant-count", type=int, default=4)
    p_auto.add_argument("--assign-limit", type=int, default=60)
    p_auto.add_argument("--stories", type=int, default=60, help="Max stories to create/assign per iteration")

    # Reels processing CLI
    p_rp = sub.add_parser("reels-process", help="Batch process videos to 9:16 reels using ffmpeg")
    p_rp.add_argument("--in-dir", type=str, default="content farm/videos")
    p_rp.add_argument("--out-dir", type=str, default="content farm/reels")
    p_rp.add_argument("--max-duration", type=int, default=58)
    p_rp.add_argument("--fps", type=int, default=30)
    p_rp.add_argument("--vbitrate", type=str, default="5M")
    p_rp.add_argument("--abitrate", type=str, default="128k")

    # Reels upload CLI
    p_ru = sub.add_parser("reels-upload", help="Upload processed reels from a directory to S3-compatible storage")
    p_ru.add_argument("--in-dir", type=str, default="content farm/reels")
    p_ru.add_argument("--prefix", type=str, default="reels/")
    p_ru.add_argument("--out-json", type=str, default=None, help="Optional JSON output path for URLs")

    # Reels schedule CLI
    p_rs = sub.add_parser("reels-schedule", help="Create 'reel' schedules from a JSON of uploaded URLs")
    p_rs.add_argument("--urls-json", type=str, required=True, help="Path to JSON with key 'uploaded' -> [urls]")
    p_rs.add_argument("--start-utc", type=str, required=True, help="UTC ISO start time e.g. 2025-08-19T14:30:00Z")
    p_rs.add_argument("--every-min", type=int, default=45, help="Spacing in minutes between reels")
    p_rs.add_argument("--priority", type=int, default=0)

    args = p.parse_args()

    if args.cmd == "scrape":
        cmd_scrape(args.subreddits, args.limit)
    elif args.cmd == "generate":
        cmd_generate(args.pool)
    elif args.cmd == "twitter-scrape":
        cmd_twitter_scrape(args.query, args.limit)
    elif args.cmd == "ocr":
        cmd_ocr(args.limit)
    elif args.cmd == "generate-variants":
        cmd_generate_variants(args.variant_count, args.limit, args.pool)
    elif args.cmd == "schedule":
        cmd_schedule(args.per_posts)
    elif args.cmd == "post-due":
        cmd_post_due(args.max_posts)
    elif args.cmd == "plan-day":
        cmd_plan_day(args.memes, args.stories, args.reels)
    elif args.cmd == "post-due-all":
        cmd_post_due_all(args.max_items)
    elif args.cmd == "assign-memes":
        cmd_assign_memes(args.limit)
    elif args.cmd == "assign-memes-variants":
        cmd_assign_memes_variants(args.limit)
    elif args.cmd == "seed-hashtags":
        cmd_seed_hashtags()
    elif args.cmd == "gen-assign-stories":
        cmd_gen_assign_stories(args.max_create)
    elif args.cmd == "plan-week":
        cmd_plan_week(args.days, args.meme_jitter, args.story_jitter, args.reel_jitter)
    elif args.cmd == "export-week-plan":
        cmd_export_week(args.out, args.days)
    elif args.cmd == "ingest-week-plan":
        cmd_ingest_week(args.path, args.meme_jitter, args.story_jitter, args.reel_jitter)
    elif args.cmd == "export-story-prompts":
        cmd_export_story_prompts(args.out)
    elif args.cmd == "export-caption-frameworks":
        cmd_export_caption_frameworks(args.out)
    elif args.cmd == "fetch-insights":
        cmd_fetch_insights(args.since)
    elif args.cmd == "auto-run":
        cmd_auto_run(
            setup=args.setup,
            loop_sleep_sec=args.sleep,
            scrape_limit=args.scrape_limit,
            twitter_query=args.twitter_query,
            twitter_limit=args.twitter_limit,
            variant_count=args.variant_count,
            assign_limit=args.assign_limit,
            story_create=args.stories,
        )
    elif args.cmd == "reels-process":
        cmd_reels_process(
            in_dir=args.in_dir,
            out_dir=args.out_dir,
            max_duration=args.max_duration,
            fps=args.fps,
            vbitrate=args.vbitrate,
            abitrate=args.abitrate,
        )
    elif args.cmd == "reels-upload":
        cmd_reels_upload(args.in_dir, args.prefix, args.out_json)
    elif args.cmd == "reels-schedule":
        cmd_reels_schedule(args.urls_json, args.start_utc, args.every_min, args.priority)
    elif args.cmd == "create-carousel":
        cmd_create_carousel(args.meme_ids, args.caption)
    elif args.cmd == "schedule-carousel":
        cmd_schedule_carousel(args.carousel_id, args.when, args.priority)
    elif args.cmd == "build-carousel":
        cmd_build_carousel(args.in_dir, args.out_dir, args.prefix, args.caption)
    elif args.cmd == "build-hashtag-pool":
        cmd_build_hashtag_pool(args.name, args.subreddits, args.twitter_query, args.top_n_trends, args.max_tags)
    elif args.cmd == "trending-audio":
        cmd_trending_audio(args.file, args.top, args.out, args.to_pool, args.csv_out)
    elif args.cmd == "build-audio-pool":
        rows = TrendingAudioAnalyzer().top_from_file(args.file, top_n=args.top)
        db.init_db()
        db.upsert_audio_pool(args.name, json.dumps(rows, ensure_ascii=False))
        print(json.dumps({"pool": args.name, "count": len(rows)}, ensure_ascii=False, indent=2))
    elif args.cmd == "engage":
        cmd_engage(args.since, args.max_replies)
    elif args.cmd == "reels-pipeline":
        cmd_reels_pipeline(
            in_dir=args.in_dir,
            out_dir=args.out_dir,
            max_duration=args.max_duration,
            fps=args.fps,
            vbitrate=args.vbitrate,
            abitrate=args.abitrate,
            prefix=args.prefix,
            start_utc=args.start_utc,
            every_min=args.every_min,
            priority=args.priority,
            out_json=args.out_json,
            pool=args.pool,
        )


if __name__ == "__main__":
    main()
