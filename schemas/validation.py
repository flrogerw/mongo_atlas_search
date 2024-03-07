import os
from dotenv import load_dotenv
from pydantic import BaseModel, UUID5, UUID4, StrictBool, PastDatetime, PositiveInt, PositiveFloat
from typing import Union, ClassVar, Optional

load_dotenv()
LANGUAGES = os.getenv('LANGUAGES').split(",")


class Podcast(BaseModel):
    podcast_uuid: UUID5 | UUID4
    rss_url: str
    original_url: str
    language: ClassVar[list[str]] = LANGUAGES
    is_explicit: StrictBool
    publisher: str
    image_url: str
    description_cleaned: str
    title_cleaned: str
    readability: int
    description_selected: PositiveInt
    advanced_popularity: PositiveFloat
    record_hash: str
    episode_count: int
    listen_score_global: Union[str, float]


class Episode(BaseModel):
    episode_uuid: UUID5 | UUID4
    episode_url: str
    podcast_id: PositiveInt
    duration: PositiveInt
    file_type: str
    language: str
    is_explicit: StrictBool
    publisher: str
    # "image_url": record['artwork_thumbnail'],
    description_cleaned: str
    title_cleaned: str
    readability: int
    description_selected: PositiveInt
    advanced_popularity: PositiveFloat
    index_status: PositiveInt
    record_hash: str
    publish_date: Optional[PastDatetime]
