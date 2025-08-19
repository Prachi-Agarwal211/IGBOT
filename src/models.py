from dataclasses import dataclass

@dataclass
class Meme:
    id: int
    source: str
    source_id: str
    title: str
    image_url: str
    caption: str | None
    hashtags: str | None
    status: str
    scheduled_time: str | None
