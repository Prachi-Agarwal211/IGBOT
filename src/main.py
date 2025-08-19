import argparse
from datetime import datetime
from typing import List
from datetime import timedelta
import pytz
import time

from . import db
from .config import DEFAULT_SUBREDDITS
from .scraper.reddit_scraper import scrape_subreddits
from .scraper.twitter_scraper import scrape_twitter_images
from .processor.captioner import generate_caption_hashtags
from .processor.captioner import generate_caption_variants
from .scheduler.scheduler import (
    next_best_slot,
    plan_day,
    assign_memes_to_open_slots,
    assign_memes_with_variants,
    create_and_assign_stories_to_open_slots,
    plan_week,
    export_week_plan_json,
    ingest_week_plan_json,
)
from .publisher.instagram_client import InstagramClient
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


def cmd_generate():
    db.init_db()
    items = db.fetch_memes_by_status("new", limit=100)
    print(f"Generating captions for {len(items)} memes...")
    for (meme_id, source, source_id, title, image_url, *_rest) in items:
        caption, hashtags = generate_caption_hashtags(title, source)
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


def cmd_generate_variants(variant_count: int, limit: int):
    db.init_db()
    items = db.fetch_new_memes_with_ocr(limit=limit)
    print(f"Generating up to {variant_count} variants for {len(items)} memes...")
    for (meme_id, source, source_id, title, image_url, ocr_text) in items:
        context = (title or "")
        if ocr_text:
            context = f"{context}\nText on meme:\n{ocr_text}" if context else f"Text on meme:\n{ocr_text}"
        try:
            variants = generate_caption_variants(context_text=context, category=None, variant_count=variant_count)
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
            post_id = ig.post_photo(image_url, f"{caption}\n\n{hashtags}" if hashtags else caption)
            db.mark_published(meme_id, now_iso)
            print(f"Posted id={meme_id} -> IG media {post_id}")
        except Exception as e:
            db.mark_failed(meme_id, str(e))
            print(f"Failed id={meme_id}: {e}")


def cmd_plan_day(memes: int, stories: int):
    db.init_db()
    plan_day(count_memes=memes, count_stories=stories)
    print(f"Planned {memes} meme slots and {stories} story slots with jitter in schedules table.")


def cmd_post_due_all(max_items: int | None = None):
    """Process unified schedules (memes + stories). Stories are stubbed for now."""
    db.init_db()
    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = db.fetch_due_schedules(now_iso=now_iso, kind=None, limit=max_items)
    if not rows:
        print("No schedules due.")
        return
    ig = InstagramClient()
    for (schedule_id, kind, meme_id, story_id, caption_variant_no, _when) in rows:
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
                # Instagram max caption ~2200 chars; we won't hard-trim here, but could if needed
                caption_full = base_caption or ""
                if tags_combined:
                    caption_full = f"{caption_full}\n\n{tags_combined}" if caption_full else tags_combined
                media_id = ig.post_photo(image_url, caption_full)
                db.mark_schedule_posted(schedule_id, now_iso, platform_post_id=media_id)
                print(f"Posted schedule={schedule_id} meme_id={meme_id} -> {media_id}")
            elif kind == 'story':
                # Placeholder: Stories posting not implemented yet
                db.mark_schedule_posted(schedule_id, now_iso, platform_post_id="")
                print(f"Marked story schedule={schedule_id} as posted (placeholder)")
            elif kind == 'reel' and meme_id:
                # For reels, we expect meme.image_url to be a video URL; if not, skip for now
                m = db.get_meme(meme_id)
                if not m:
                    raise RuntimeError(f"Meme {meme_id} missing for reel")
                _, media_url, base_caption, base_tags = m
                rotated = _rotate_hashtags(schedule_id)
                tags_combined = " ".join([t for t in [base_tags or "", rotated] if t]).strip()
                caption_full = base_caption or ""
                if tags_combined:
                    caption_full = f"{caption_full}\n\n{tags_combined}" if caption_full else tags_combined
                try:
                    media_id = ig.post_reel(media_url, caption_full)
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


def cmd_plan_week(days: int, meme_jitter: int, story_jitter: int):
    db.init_db()
    plan_week(days=days, meme_jitter_min=meme_jitter, story_jitter_min=story_jitter)
    print(f"Planned {days} day(s) with fixed windows and jitter.")


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

    p_tw = sub.add_parser("twitter-scrape", help="Scrape memes from Twitter via API v2")
    p_tw.add_argument("--query", type=str, default="(meme OR memes) (india OR indian) lang:en -is:retweet has:images")
    p_tw.add_argument("--limit", type=int, default=50)

    p_ocr = sub.add_parser("ocr", help="Extract text from meme images using Tesseract")
    p_ocr.add_argument("--limit", type=int, default=50)

    p_gv = sub.add_parser("generate-variants", help="Generate 3-5 caption variants per meme and store them")
    p_gv.add_argument("--variant-count", type=int, default=3)
    p_gv.add_argument("--limit", type=int, default=50)

    p_sched = sub.add_parser("schedule", help="Schedule ready posts")
    p_sched.add_argument("--per-posts", type=int, default=3)

    p_post = sub.add_parser("post-due", help="Publish due posts to Instagram")
    p_post.add_argument("--max-posts", type=int, default=None)

    p_plan = sub.add_parser("plan-day", help="Create randomized daily schedule entries for memes and stories")
    p_plan.add_argument("--memes", type=int, default=24)
    p_plan.add_argument("--stories", type=int, default=48)

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

    p_auto = sub.add_parser("auto-run", help="Run full pipeline in a loop (one command daemon)")
    p_auto.add_argument("--setup", action="store_true", help="Seed hashtags once before starting loop")
    p_auto.add_argument("--sleep", type=int, default=300, help="Sleep seconds between iterations (default 300)")
    p_auto.add_argument("--scrape-limit", type=int, default=60)
    p_auto.add_argument("--twitter-query", type=str, default="meme OR funny -nsfw")
    p_auto.add_argument("--twitter-limit", type=int, default=60)
    p_auto.add_argument("--variant-count", type=int, default=4)
    p_auto.add_argument("--assign-limit", type=int, default=60)
    p_auto.add_argument("--stories", type=int, default=60, help="Max stories to create/assign per iteration")

    args = p.parse_args()

    if args.cmd == "scrape":
        cmd_scrape(args.subreddits, args.limit)
    elif args.cmd == "generate":
        cmd_generate()
    elif args.cmd == "twitter-scrape":
        cmd_twitter_scrape(args.query, args.limit)
    elif args.cmd == "ocr":
        cmd_ocr(args.limit)
    elif args.cmd == "generate-variants":
        cmd_generate_variants(args.variant_count, args.limit)
    elif args.cmd == "schedule":
        cmd_schedule(args.per_posts)
    elif args.cmd == "post-due":
        cmd_post_due(args.max_posts)
    elif args.cmd == "plan-day":
        cmd_plan_day(args.memes, args.stories)
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
        cmd_plan_week(args.days, args.meme_jitter, args.story_jitter)
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


if __name__ == "__main__":
    main()
