from typing import List
import os
import tweepy
from ..config import TWITTER_BEARER_TOKEN
from .. import db


def init_twitter_client() -> tweepy.Client:
    token = TWITTER_BEARER_TOKEN
    if not token:
        raise RuntimeError("Missing TWITTER_BEARER_TOKEN in .env")
    return tweepy.Client(bearer_token=token, wait_on_rate_limit=True)


def extract_image_urls(includes_media) -> List[str]:
    urls: List[str] = []
    if not includes_media:
        return urls
    for m in includes_media:
        if m.type == "photo" and m.url:
            urls.append(m.url)
    return urls


def scrape_twitter_images(query: str = "(meme OR memes) (india OR indian) lang:en -is:retweet has:images", max_results: int = 50) -> int:
    """Search recent popular tweets with images relevant to Indian memes and store as memes."""
    client = init_twitter_client()
    resp = client.search_recent_tweets(
        query=query,
        max_results=min(max_results, 100),
        expansions=["attachments.media_keys"],
        media_fields=["url", "type"],
        sort_order="recency",
    )
    if not resp.data:
        return 0

    media_map = {m.media_key: m for m in (resp.includes.get("media", []) if resp.includes else [])}
    inserted = 0
    for tweet in resp.data:
        media_keys = getattr(tweet, "attachments", {}).get("media_keys", []) if hasattr(tweet, "attachments") and tweet.attachments else []
        if not media_keys:
            continue
        images = [media_map[k] for k in media_keys if k in media_map]
        image_urls = extract_image_urls(images)
        if not image_urls:
            continue
        title = getattr(tweet, "text", "")
        # insert one row per image for simplicity
        for url in image_urls:
            ok = db.insert_meme(
                source="twitter",
                source_id=str(tweet.id),
                title=title[:250],
                image_url=url,
            )
            if ok:
                inserted += 1
    return inserted
