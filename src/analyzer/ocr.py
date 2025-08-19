from typing import Optional
import io
import requests
from PIL import Image, ImageOps, ImageFilter
import pytesseract
from ..config import TESSERACT_CMD

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


def extract_text_from_url(image_url: str) -> str:
    img = fetch_image(image_url)
    pre = preprocess(img)
    text = pytesseract.image_to_string(pre, lang="eng")
    return (text or "").strip()
