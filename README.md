# IG Meme Content Farm (India)

A Windows-friendly Python bot that:
- Scrapes fresh Indian memes from Reddit.
- Generates catchy captions + trending hashtags via Gemini.
- Schedules posts for peak Indian times.
- Auto-posts to Instagram via Instagram Graph API (Business/Creator account required).

## Quick Start

1) Create a Facebook App and connect an Instagram Business/Creator Account.
   - Obtain: PAGE_ACCESS_TOKEN (with instagram_basic, pages_show_list, pages_manage_posts, instagram_content_publish) and IG_BUSINESS_ACCOUNT_ID.
   - Docs: https://developers.facebook.com/docs/instagram-api/guides/content-publishing

2) Create Reddit API credentials (script app) at https://www.reddit.com/prefs/apps
   - Get: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET

3) Get Gemini API key: https://aistudio.google.com/app/apikey

4) Copy `.env.example` to `.env` and fill your keys.

5) Install deps:
```
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

6) First run (populate DB and queue posts):
```
python -m src.main scrape --subreddits r/IndianDankMemes r/DesiMemes --limit 30
python -m src.main generate
python -m src.main schedule --per-posts 6
```

7) Posting loop (run periodically via Windows Task Scheduler):
```
python -m src.main post-due
```
Run this every 10 minutes. It will publish any queued posts whose scheduled time has arrived.

## Commands
- `python -m src.main scrape --subreddits ... --limit N` Scrape memes from Reddit (image posts only).
- `python -m src.main generate` Generate captions/hashtags for scraped memes that need it.
- `python -m src.main schedule --per-posts N` Queue up to N posts with optimal Indian times.
- `python -m src.main post-due` Publish any due posts to Instagram.

## Notes
- This MVP posts photos using `image_url` (no local upload). The URL must be public (Reddit is fine).
- Avoid scraping Instagram directly (ToS).
- Respect copyright. Prefer original/transformative captions and credit sources in captions where possible.

## Environment
See `.env.example` for all variables. Timezone assumed IST (Asia/Kolkata).
