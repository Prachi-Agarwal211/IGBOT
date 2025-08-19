from __future__ import annotations
import json
import csv
from collections import Counter
from typing import Iterable


def _iter_audio_entries_from_file(path: str) -> Iterable[str]:
    """Yield audio identifiers or URLs from a file.
    Supports:
      - .json -> ["url_or_id", ...]
      - .csv -> header optional; uses column named 'audio' if present else first column
      - .txt/.list -> one entry per line
    """
    p = path.lower()
    if p.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for x in data:
                if isinstance(x, str) and x.strip():
                    yield x.strip()
        elif isinstance(data, dict):
            # try common keys
            for key in ("audios", "audio", "items"):
                arr = data.get(key)
                if isinstance(arr, list):
                    for x in arr:
                        if isinstance(x, str) and x.strip():
                            yield x.strip()
    elif p.endswith(".csv"):
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            return
        header = rows[0]
        start = 1
        col_idx = 0
        if any(h.strip().lower() == "audio" for h in header):
            # find first 'audio' column
            for i, h in enumerate(header):
                if h.strip().lower() == "audio":
                    col_idx = i
                    break
        else:
            # no header assumed
            start = 0
        for r in rows[start:]:
            if not r:
                continue
            v = (r[col_idx] or "").strip()
            if v:
                yield v
    else:
        # treat as text/lines
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    yield s


def _normalize_audio_token(s: str) -> str:
    """Return a normalized audio token for counting.
    For Instagram audio links, keep path segment and drop query/fragment.
    If only an ID is given, return as-is.
    Examples accepted:
      https://www.instagram.com/audio/123456789012345/ -> instagram:audio:123456789012345
      https://www.instagram.com/reel/ABCDEF... -> left as the reel URL if audio not provided explicitly
    """
    t = s.strip()
    # strip tracking query/fragment
    for sep in ("?", "#"):
        if sep in t:
            t = t.split(sep, 1)[0]
    if "/audio/" in t:
        try:
            after = t.split("/audio/", 1)[1]
            audio_id = after.strip("/")
            # strip trailing path parts if any
            audio_id = audio_id.split("/")[0]
            if audio_id:
                return f"instagram:audio:{audio_id}"
        except Exception:
            pass
    return t


class TrendingAudioAnalyzer:
    """MVP analyzer that aggregates provided audio URLs/IDs and returns top-N.
    This does not scrape Instagram; it operates on user-provided inputs.
    """

    def top_from_file(self, path: str, top_n: int = 25) -> list[dict]:
        tokens = (_normalize_audio_token(s) for s in _iter_audio_entries_from_file(path))
        counter = Counter(tokens)
        most = counter.most_common(top_n)
        return [{"audio": k, "count": int(v)} for k, v in most]

    def top_from_list(self, items: list[str], top_n: int = 25) -> list[dict]:
        tokens = (_normalize_audio_token(s) for s in items if isinstance(s, str))
        counter = Counter(tokens)
        most = counter.most_common(top_n)
        return [{"audio": k, "count": int(v)} for k, v in most]
