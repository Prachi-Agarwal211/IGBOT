import os
from dotenv import load_dotenv

load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "ig-meme-farm/0.1")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

IG_PAGE_ACCESS_TOKEN = os.getenv("IG_PAGE_ACCESS_TOKEN", "")
IG_BUSINESS_ACCOUNT_ID = os.getenv("IG_BUSINESS_ACCOUNT_ID", "")

DEFAULT_SUBREDDITS = [s.strip() for s in os.getenv("DEFAULT_SUBREDDITS", "r/IndianDankMemes").split(",") if s.strip()]
POSTS_PER_DAY = int(os.getenv("POSTS_PER_DAY", "3"))

DB_PATH = os.path.join(os.path.dirname(__file__), "meme_farm.sqlite3")

TIMEZONE = "Asia/Kolkata"

# v2 additions
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "")  # Optional: path to tesseract.exe on Windows
