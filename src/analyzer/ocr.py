from typing import Optional
import io
import requests
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from ..config import TESSERACT_CMD, OCR_PROVIDER, OCRSPACE_API_KEY

# Configure tesseract path on Windows if provided
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD


def fetch_image(url: str) -> Image.Image:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    img = Image.open(io.BytesIO(r.content))
    return img.convert("RGB")


def preprocess(img: Image.Image) -> Image.Image:
    # Simple, fast preproc: grayscale, autocontrast, slight sharpen, resize if small
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.SHARPEN)
    if min(g.size) < 600:
        scale = 600 / min(g.size)
        g = g.resize((int(g.width * scale), int(g.height * scale)))
    return g


def _extract_text_local(image_url: str) -> str:
    img = fetch_image(image_url)
    pre = preprocess(img)
    text = pytesseract.image_to_string(pre, lang="eng")
    return (text or "").strip()


def _extract_text_ocrspace(image_url: str) -> str:
    """Use OCR.Space API to extract text from an image URL.
    Docs: https://ocr.space/ocrapi
    """
    if not OCRSPACE_API_KEY:
        raise RuntimeError("OCRSPACE_API_KEY not set")
    endpoint = "https://api.ocr.space/parse/image"
    data = {
        "apikey": OCRSPACE_API_KEY,
        "url": image_url,
        "language": "eng",
        "isOverlayRequired": False,
        "OCREngine": 2,  # Best available free engine
    }
    resp = requests.post(endpoint, data=data, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected OCR response")
    if payload.get("IsErroredOnProcessing"):
        msg = payload.get("ErrorMessage") or payload.get("ErrorDetails") or "OCR processing error"
        if isinstance(msg, list):
            msg = "; ".join(str(m) for m in msg)
        raise RuntimeError(f"OCR.Space error: {msg}")
    results = payload.get("ParsedResults") or []
    texts: list[str] = []
    for r in results:
        t = (r or {}).get("ParsedText") or ""
        if t:
            texts.append(str(t))
    return "\n".join(texts).strip()


def extract_text_from_url(image_url: str) -> str:
    # Route based on provider; fallback gracefully
    provider = (OCR_PROVIDER or "local").lower()
    if provider == "ocrspace":
        try:
            return _extract_text_ocrspace(image_url)
        except Exception as e:
            # Fallback to local if configured
            if TESSERACT_CMD:
                try:
                    return _extract_text_local(image_url)
                except Exception:
                    pass
            raise
    # default local
    return _extract_text_local(image_url)
