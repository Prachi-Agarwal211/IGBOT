import requests
from typing import Optional
from ..config import IG_PAGE_ACCESS_TOKEN, IG_BUSINESS_ACCOUNT_ID

GRAPH_BASE = "https://graph.facebook.com/v19.0"

class InstagramClient:
    def __init__(self, page_access_token: Optional[str] = None, ig_business_account_id: Optional[str] = None):
        self.token = page_access_token or IG_PAGE_ACCESS_TOKEN
        self.igid = ig_business_account_id or IG_BUSINESS_ACCOUNT_ID
        if not (self.token and self.igid):
            raise RuntimeError("Missing IG_PAGE_ACCESS_TOKEN or IG_BUSINESS_ACCOUNT_ID in .env")

    def create_photo_container(self, image_url: str, caption: str) -> str:
        url = f"{GRAPH_BASE}/{self.igid}/media"
        payload = {
            "image_url": image_url,
            "caption": caption,
            "access_token": self.token,
        }
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data["id"]

    def publish_container(self, creation_id: str) -> str:
        url = f"{GRAPH_BASE}/{self.igid}/media_publish"
        payload = {
            "creation_id": creation_id,
            "access_token": self.token,
        }
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("id", "")

    def post_photo(self, image_url: str, caption: str) -> str:
        creation_id = self.create_photo_container(image_url, caption)
        return self.publish_container(creation_id)

    # Carousel support (multiple images)
    def create_carousel_item(self, image_url: str) -> str:
        url = f"{GRAPH_BASE}/{self.igid}/media"
        payload = {
            "image_url": image_url,
            "is_carousel_item": "true",
            "access_token": self.token,
        }
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
        return r.json()["id"]

    def create_carousel_container(self, children_creation_ids: list[str], caption: str) -> str:
        url = f"{GRAPH_BASE}/{self.igid}/media"
        payload = {
            "media_type": "CAROUSEL",
            "children": ",".join(children_creation_ids),
            "caption": caption,
            "access_token": self.token,
        }
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
        return r.json()["id"]

    def post_carousel(self, image_urls: list[str], caption: str) -> str:
        if not image_urls or len(image_urls) < 2:
            raise ValueError("Carousel requires at least 2 image URLs")
        children = [self.create_carousel_item(u) for u in image_urls]
        creation_id = self.create_carousel_container(children, caption)
        return self.publish_container(creation_id)

    # Reels support (video upload)
    def create_reel_container(self, video_url: str, caption: str, cover_url: str | None = None) -> str:
        """
        Uses IG Graph API to create a REELS media container.
        Note: Your app must have permissions and the account must be eligible for Reels publishing.
        """
        url = f"{GRAPH_BASE}/{self.igid}/media"
        payload = {
            "media_type": "REELS",
            "video_url": video_url,
            "caption": caption,
            "access_token": self.token,
        }
        if cover_url:
            payload["thumb_offset"] = 0  # optional; placeholder if needed later
            payload["cover_url"] = cover_url
        r = requests.post(url, data=payload, timeout=300)
        r.raise_for_status()
        return r.json()["id"]

    def post_reel(self, video_url: str, caption: str, cover_url: str | None = None) -> str:
        creation_id = self.create_reel_container(video_url, caption, cover_url)
        return self.publish_container(creation_id)

    # Stories support (image story)
    def create_story_container(self, image_url: str, caption: str | None = None) -> str:
        """
        Placeholder for Story publishing via IG Graph API.
        As of current implementation, interactive stickers (polls, quizzes, sliders) are not exposed here.
        Implement when your app has access to Stories publishing. For now, this raises to avoid silent failure.
        """
        raise NotImplementedError("Stories publishing via API not implemented in this client. Prepare media manually or extend with official endpoint when available.")

    def post_story(self, image_url: str, caption: str | None = None) -> str:
        creation_id = self.create_story_container(image_url, caption)
        return self.publish_container(creation_id)

    # Insights/metadata
    def get_media(self, media_id: str, fields: str = "id,media_type,like_count,comments_count,caption") -> dict:
        url = f"{GRAPH_BASE}/{media_id}"
        params = {
            "fields": fields,
            "access_token": self.token,
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def get_media_insights(self, media_id: str, metrics: list[str]) -> dict:
        """
        Fetch media insights. Metrics vary by media type.
        Example metrics: impressions, reach, profile_activity, saves, shares, plays.
        """
        url = f"{GRAPH_BASE}/{media_id}/insights"
        params = {
            "metric": ",".join(metrics),
            "access_token": self.token,
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        # Normalize into {metric: value}
        out = {}
        for item in data.get("data", []):
            name = item.get("name")
            values = item.get("values") or []
            if values:
                out[name] = values[0].get("value")
        return out

    # Comments (first-comment automation)
    def create_comment(self, media_id: str, message: str) -> str:
        """Create a comment on a published media. Returns the comment id."""
        url = f"{GRAPH_BASE}/{media_id}/comments"
        payload = {
            "message": message,
            "access_token": self.token,
        }
        r = requests.post(url, data=payload, timeout=60)
        r.raise_for_status()
        return r.json().get("id", "")
