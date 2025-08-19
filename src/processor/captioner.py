from typing import Tuple, List, Optional
import google.generativeai as genai
from ..config import GEMINI_API_KEY, GEMINI_MODEL
from .. import db


def init_gemini():
    if not GEMINI_API_KEY:
        raise RuntimeError("Missing GEMINI_API_KEY in .env")
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel(GEMINI_MODEL)


def generate_caption_hashtags(title: str, source: str = "reddit", pool_name: Optional[str] = None) -> Tuple[str, str]:
    """Generate a crisp caption and 10-15 Indian trending hashtags.
    Returns (caption, hashtags_string)
    """
    model = init_gemini()
    prompt = f"""
    You are an expert Indian meme copywriter for Instagram.
    Input meme context/title: "{title}"

    Tasks:
    1) Write a short, witty, relatable caption in Hinglish (avoid offensive slurs). Keep within 120 chars.
    2) Provide 10-15 Indian trending hashtags that fit Instagram (no spaces, use #, include mix like #desimemes #relatable #indiandank #hindimemes #memepage #trending #reels).
    3) Avoid quotes and emojis overuse; 1-2 emojis max.

    Output format (strict):
    CAPTION: <caption text>
    HASHTAGS: #tag1 #tag2 #tag3 ...
    """
    resp = model.generate_content(prompt)
    text = resp.text.strip()

    caption = ""
    hashtags = ""
    for line in text.splitlines():
        if line.upper().startswith("CAPTION:"):
            caption = line.split(":", 1)[1].strip()
        elif line.upper().startswith("HASHTAGS:"):
            hashtags = line.split(":", 1)[1].strip()
    if not caption:
        caption = title[:100]
    if not hashtags:
        hashtags = "#desimemes #indiandank #relatable #hindimemes #meme #trending"
    # enrich from hashtag pool if provided
    if pool_name:
        pool_csv = db.get_hashtag_pool(pool_name)
        if pool_csv:
            pool_tags = [t.strip() for t in pool_csv.split(',') if t.strip()]
            base = [t for t in hashtags.split() if t.startswith('#')]
            combined = []
            seen = set()
            for t in base + [('#' + t.lstrip('#')) for t in pool_tags]:
                k = t.lower()
                if not k.startswith('#') or k in seen:
                    continue
                seen.add(k)
                combined.append(t)
                if len(combined) >= 28:  # leave room for up to 2 manual tags
                    break
            hashtags = ' '.join(combined)
    return caption, hashtags


def generate_caption_variants(context_text: str, category: str | None = None, variant_count: int = 3, pool_name: Optional[str] = None) -> List[Tuple[str, str]]:
    """Return list of (caption, hashtags) variants. 3–5 recommended.
    context_text: title + OCR text or any enriched context.
    """
    variant_count = max(3, min(5, variant_count))
    model = init_gemini()
    cat_hint = f"Category: {category}." if category else ""
    prompt = f"""
    You are a top-tier Indian meme caption writer. {cat_hint}
    Use Hinglish, avoid slurs, <=120 chars per caption, 1-2 emojis max.
    Generate {variant_count} strong, distinct caption options for this meme context.
    Context:\n{context_text}\n
    Also provide 10-15 hashtags per option. Mix trending (#indiandank, #hindimemes, #bollywoodmemes, #iplmemes), evergreen (#relatable #memepage), and niche inferred from context.

    Output STRICTLY as blocks separated by a line with "---":
    CAPTION: <caption>
    HASHTAGS: #tag1 #tag2 ...
    ---
    CAPTION: <caption>
    HASHTAGS: #tag1 #tag2 ...
    """
    resp = model.generate_content(prompt)
    text = (resp.text or "").strip()
    blocks = [b.strip() for b in text.split("---") if b.strip()]
    variants: List[Tuple[str, str]] = []
    for b in blocks[:variant_count]:
        cap = ""
        tags = ""
        for line in b.splitlines():
            if line.upper().startswith("CAPTION:"):
                cap = line.split(":", 1)[1].strip()
            elif line.upper().startswith("HASHTAGS:"):
                tags = line.split(":", 1)[1].strip()
        if cap:
            tags_out = tags or "#desimemes #indiandank #relatable"
            if pool_name:
                pool_csv = db.get_hashtag_pool(pool_name)
                if pool_csv:
                    pool_tags = [t.strip() for t in pool_csv.split(',') if t.strip()]
                    base = [t for t in tags_out.split() if t.startswith('#')]
                    combined = []
                    seen = set()
                    for t in base + [('#' + t.lstrip('#')) for t in pool_tags]:
                        k = t.lower()
                        if not k.startswith('#') or k in seen:
                            continue
                        seen.add(k)
                        combined.append(t)
                        if len(combined) >= 28:
                            break
                    tags_out = ' '.join(combined)
            variants.append((cap, tags_out))
    if not variants:
        variants.append((context_text[:100], "#desimemes #indiandank #relatable"))
    return variants
