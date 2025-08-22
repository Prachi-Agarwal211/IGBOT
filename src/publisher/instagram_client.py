import os
import tempfile
import requests
from typing import Optional, List

from instagrapi import Client
from instagrapi.exceptions import LoginRequired

from ..config import (
    INSTAGRAM_USERNAME,
    INSTAGRAM_PASSWORD,
    INSTAGRAM_SESSION_FILE,
)


class InstagramClient:
    """
    Instagram client using instagrapi (username/password login).
    Methods mirror the previous Graph API client so existing code keeps working:
      - post_photo(image_url: str, caption: str) -> str (returns media id)
      - post_carousel(image_urls: list[str], caption: str) -> str
      - post_reel(video_url: str, caption: str) -> str
      - create_comment(media_id: str, message: str) -> str
    """

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self.username = username or INSTAGRAM_USERNAME
        self.password = password or INSTAGRAM_PASSWORD
        if not (self.username and self.password):
            raise RuntimeError("Missing INSTAGRAM_USERNAME or INSTAGRAM_PASSWORD in .env")
        self.client = Client()
        self._login()

    # ----- Auth/session -----
    def _login(self):
        session_path = INSTAGRAM_SESSION_FILE
        try:
            if session_path and os.path.exists(session_path):
                print("Loading existing Instagram session...")
                self.client.load_settings(session_path)
                self.client.login(self.username, self.password)
                # Validate session
                try:
                    _ = self.client.get_timeline_feed()
                    print("Instagram session valid.")
                    return
                except LoginRequired:
                    print("Instagram session expired. Re-authenticating...")
            # Fresh login
            self.client.login(self.username, self.password)
            if session_path:
                self.client.dump_settings(session_path)
                print(f"Saved Instagram session to {session_path}")
        except Exception as e:
            # Last attempt: fresh login without cached settings
            print(f"Instagram login issue: {e}. Trying clean login...")
            self.client = Client()
            self.client.login(self.username, self.password)
            if session_path:
                self.client.dump_settings(session_path)

    # ----- Helpers -----
    def _download_to_temp(self, url: str, suffix: str) -> str:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        fd, path = tempfile.mkstemp(suffix=suffix)
        with os.fdopen(fd, "wb") as f:
            f.write(r.content)
        return path

    # ----- Public API (mirrors old client) -----
    def post_photo(self, image_url: str, caption: str) -> str:
        """Uploads a photo from a remote URL and returns media id."""
        path = None
        try:
            # Try to infer extension; default to .jpg
            ext = ".jpg"
            low = image_url.lower()
            for e in [".jpg", ".jpeg", ".png", ".webp"]:
                if e in low:
                    ext = e
                    break
            path = self._download_to_temp(image_url, suffix=ext)
            media = self.client.photo_upload(path, caption)
            return str(getattr(media, "id", ""))
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def post_carousel(self, image_urls: List[str], caption: str) -> str:
        if not image_urls or len(image_urls) < 2:
            raise ValueError("Carousel requires at least 2 image URLs")
        paths: List[str] = []
        try:
            # Download all images
            for u in image_urls:
                ext = ".jpg"
                low = u.lower()
                for e in [".jpg", ".jpeg", ".png", ".webp"]:
                    if e in low:
                        ext = e
                        break
                paths.append(self._download_to_temp(u, suffix=ext))
            media = self.client.album_upload(paths, caption)
            return str(getattr(media, "id", ""))
        finally:
            for p in paths:
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass

    def post_reel(self, video_url: str, caption: str) -> str:
        path = None
        try:
            ext = ".mp4"
            if ".mov" in video_url.lower():
                ext = ".mov"
            path = self._download_to_temp(video_url, suffix=ext)
            media = self.client.clip_upload(path, caption)
            return str(getattr(media, "id", ""))
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def create_comment(self, media_id: str, message: str) -> str:
        if not media_id or not message:
            return ""
        c = self.client.media_comment(media_id=media_id, text=message)
        # instagrapi returns dict-like with pk/id
        return str(getattr(c, "pk", "") or getattr(c, "id", ""))
