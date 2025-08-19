import os
from typing import List, Dict, Any
from pathlib import Path

from yt_dlp import YoutubeDL


def download_videos(query: str, max_videos: int = 10, out_dir: str = "content farm/videos") -> List[Dict[str, Any]]:
    """Download short videos for a query using yt-dlp's search. Returns list of metadata dicts.

    - Uses ytsearch to find videos relevant to the query.
    - Saves files under out_dir using video title + id.
    - Skips existing files.
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Use ytsearch and prefer shorts-like durations (< 90s) via a postprocessor filter when available
    # Note: duration filtering relies on extractor's metadata; we also re-check client-side.
    ydl_opts = {
        "outtmpl": os.path.join(out_dir, "%(title).80s-%(id)s.%(ext)s"),
        "noplaylist": True,
        "ignoreerrors": True,
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
        # Best mp4 video+audio
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        # Limit file size to something IG-friendly (optional): ~100MB
        # "max_filesize": 100 * 1024 * 1024,
    }

    search_term = f"ytsearch{max_videos}:{query}"
    results: List[Dict[str, Any]] = []

    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(search_term, download=False)
        entries = info.get("entries", []) if info else []
        for e in entries:
            if not e:
                continue
            # Basic duration gating: prefer < 90s for Reels-like content
            duration = e.get("duration") or 0
            if duration and duration > 95:
                continue
            # If file already exists, skip downloading
            # Construct output name as yt-dlp would (approx)
            title = (e.get("title") or "video").strip().replace("/", "-")
            vid = e.get("id") or "id"
            # We don't know ext until after selection; allow mp4 common case
            expected_glob = list(Path(out_dir).glob(f"{title[:80]}-{vid}.*"))
            if expected_glob:
                filepath = str(expected_glob[0])
            else:
                # Download this entry specifically
                single = ydl.extract_info(e.get("webpage_url") or e.get("url"), download=True)
                # Determine file path from result
                filepath = ydl.prepare_filename(single)
                # If mkv or webm, leave as is for now (later processing can transcode)
            results.append({
                "id": vid,
                "title": e.get("title"),
                "duration": duration,
                "uploader": e.get("uploader"),
                "url": e.get("webpage_url") or e.get("url"),
                "filepath": filepath,
            })
    return results
