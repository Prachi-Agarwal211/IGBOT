from typing import Iterable, List
import praw
from ..config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
from .. import db


def init_reddit():
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing Reddit credentials. Set REDDIT_CLIENT_ID/SECRET/USER_AGENT in .env")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    return reddit


def is_image_post(submission) -> bool:
    url = getattr(submission, "url", "")
    if not url:
        return False
    url_lower = url.lower()
    return any(url_lower.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif"]) or "i.redd.it" in url_lower or "i.imgur.com" in url_lower


def scrape_subreddits(subreddits: List[str], limit: int = 30) -> int:
    reddit = init_reddit()
    inserted = 0
    for sub in subreddits:
        for s in reddit.subreddit(sub.replace("r/", "")).hot(limit=limit):
            if s.stickied:
                continue
            if not is_image_post(s):
                continue
            ok = db.insert_meme(
                source="reddit",
                source_id=s.id,
                title=s.title or "",
                image_url=s.url,
            )
            if ok:
                inserted += 1
    return inserted
