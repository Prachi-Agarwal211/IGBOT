from typing import Tuple, List
import google.generativeai as genai
from ..config import GEMINI_API_KEY, GEMINI_MODEL


def init_gemini():
    if not GEMINI_API_KEY:
        raise RuntimeError("Missing GEMINI_API_KEY in .env")
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel(GEMINI_MODEL)


def generate_caption_hashtags(title: str, source: str = "reddit") -> Tuple[str, str]:
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
    return caption, hashtags


def generate_caption_variants(context_text: str, category: str | None = None, variant_count: int = 3) -> List[Tuple[str, str]]:
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
            variants.append((cap, tags or "#desimemes #indiandank #relatable"))
    if not variants:
        variants.append((context_text[:100], "#desimemes #indiandank #relatable"))
    return variants
