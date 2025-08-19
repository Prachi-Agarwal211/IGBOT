import os
from typing import List
from PIL import Image

TARGET_W, TARGET_H = 1080, 1350  # 4:5 portrait recommended by IG


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _process_one(src_path: str, dst_path: str):
    img = Image.open(src_path).convert("RGB")
    # Fit inside 1080x1350 with padding (letterbox/pillarbox) and center
    img.thumbnail((TARGET_W, TARGET_H), Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", (TARGET_W, TARGET_H), color=(0, 0, 0))
    x = (TARGET_W - img.width) // 2
    y = (TARGET_H - img.height) // 2
    canvas.paste(img, (x, y))
    canvas.save(dst_path, format="JPEG", quality=90)


def process_directory(in_dir: str, out_dir: str) -> List[str]:
    """Process all images in a directory to 1080x1350 JPEGs.
    Returns list of output file paths in order.
    """
    _ensure_dir(out_dir)
    names = sorted([n for n in os.listdir(in_dir) if n.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))])
    outputs: List[str] = []
    for i, name in enumerate(names, start=1):
        src = os.path.join(in_dir, name)
        dst = os.path.join(out_dir, f"{i:02d}.jpg")
        _process_one(src, dst)
        outputs.append(dst)
    return outputs
