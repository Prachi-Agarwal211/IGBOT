import os
import time
from typing import List, Dict, Any

from pytrends.request import TrendReq
import praw
import tweepy


class TrendAnalyzer:
    """Aggregates trending topics from Google Trends (India), Reddit subreddits, and Twitter hashtags.

    Environment variables used:
    - REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
    - TWITTER_BEARER_TOKEN (v2 read-only)
    """

    def __init__(self):
        # Google Trends client
        self.pytrends = TrendReq(hl='en-US', tz=330)  # IST offset

        # Reddit client
        self.reddit = None
        rid = os.getenv("REDDIT_CLIENT_ID")
        rsecret = os.getenv("REDDIT_CLIENT_SECRET")
        ragent = os.getenv("REDDIT_USER_AGENT")
        if rid and rsecret and ragent:
            try:
                self.reddit = praw.Reddit(
                    client_id=rid,
                    client_secret=rsecret,
                    user_agent=ragent,
                )
            except Exception:
                self.reddit = None

        # Twitter client
        self.twitter = None
        bearer = os.getenv("TWITTER_BEARER_TOKEN")
        if bearer:
            try:
                self.twitter = tweepy.Client(bearer_token=bearer, wait_on_rate_limit=True)
            except Exception:
                self.twitter = None

    # ---------- Google Trends ----------
    def get_google_trends(self, top_n: int = 20) -> List[str]:
        """Return current trending searches in India (as keywords).
        Uses pytrends trending_searches for 'india'.
        """
        try:
            df = self.pytrends.trending_searches(pn='india')
            if df is None or df.empty:
                return []
            topics = [str(x).strip() for x in df[0].tolist() if str(x).strip()]
            return topics[:top_n]
        except Exception:
            return []

    # ---------- Reddit ----------
    def get_reddit_hot_posts(self, subreddits: List[str], limit: int = 100, score_min: int = 500,
                              hours: int = 24) -> List[Dict[str, Any]]:
        """Return hot/upvoted recent posts from target subreddits.
        Each item: {title, url, score, subreddit}
        """
        if not self.reddit:
            return []
        out: List[Dict[str, Any]] = []
        since_ts = time.time() - hours * 3600
        for sr in subreddits:
            name = sr.replace('r/', '').strip()
            try:
                for p in self.reddit.subreddit(name).hot(limit=limit):
                    if getattr(p, 'created_utc', 0) < since_ts:
                        continue
                    if getattr(p, 'score', 0) < score_min:
                        continue
                    out.append({
                        'title': p.title,
                        'url': p.url,
                        'score': int(p.score or 0),
                        'subreddit': name,
                    })
            except Exception:
                continue
        # Sort by score desc
        out.sort(key=lambda x: x['score'], reverse=True)
        return out

    # ---------- Twitter ----------
    def get_twitter_hashtags(self, query: str = "meme OR funny lang:en -is:retweet",
                              max_results: int = 100) -> List[Dict[str, Any]]:
        """Search recent tweets and aggregate top hashtags by frequency.
        Returns list of {hashtag, count} sorted desc.
        Note: We use v2 recent search; global trends endpoint is not publicly available in v2.
        """
        if not self.twitter:
            return []
        try:
            resp = self.twitter.search_recent_tweets(
                query=query,
                tweet_fields=["entities", "public_metrics", "lang"],
                max_results=min(max_results, 100),
            )
            counts: Dict[str, int] = {}
            if resp and resp.data:
                for t in resp.data:
                    ents = getattr(t, 'entities', None) or {}
                    tags = ents.get('hashtags') or []
                    for h in tags:
                        tag = h.get('tag')
                        if not tag:
                            continue
                        k = tag.lower()
                        counts[k] = counts.get(k, 0) + 1
            out = [{'hashtag': k, 'count': v} for k, v in counts.items()]
            out.sort(key=lambda x: x['count'], reverse=True)
            return out[:50]
        except Exception:
            return []

    # ---------- Aggregation ----------
    def aggregate(self, subreddits: List[str], twitter_query: str = "meme OR funny lang:en -is:retweet",
                  top_n_trends: int = 20) -> Dict[str, Any]:
        google = self.get_google_trends(top_n=top_n_trends)
        reddit = self.get_reddit_hot_posts(subreddits=subreddits, limit=120, score_min=300, hours=24)
        twitter = self.get_twitter_hashtags(query=twitter_query, max_results=100)
        return {
            'google_trends_in': google,
            'reddit_hot': reddit,
            'twitter_hashtags': twitter,
        }
