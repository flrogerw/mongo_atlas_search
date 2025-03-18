import os
from dotenv import load_dotenv
from pydantic import BaseModel, StrictBool, PastDatetime, PositiveInt, PositiveFloat, HttpUrl, Field
from uuid import UUID
from typing import Union, ClassVar, Optional, List

# Load environment variables
load_dotenv()

# Ensure LANGUAGES is loaded properly
LANGUAGES = os.getenv('LANGUAGES', "").split(",") if os.getenv('LANGUAGES') else ["en"]


class Podcast(BaseModel):
    podcast_uuid: UUID
    rss_url: HttpUrl  # Ensures valid URL format
    original_url: HttpUrl
    language: ClassVar[List[str]] = LANGUAGES  # Ensures a list of languages
    is_explicit: StrictBool
    publisher: str = Field(..., min_length=1)  # Ensures publisher is not empty
    image_url: HttpUrl
    description_cleaned: str = Field(..., min_length=1)  # Prevents empty descriptions
    title_cleaned: str = Field(..., min_length=1)
    readability: PositiveInt
    description_selected: PositiveInt
    advanced_popularity: int
    record_hash: str = Field(..., min_length=1)
    episode_count: PositiveInt
    listen_score_global: Union[str, PositiveFloat]  # Ensures listen score is either a string or a positive float


class Episode(BaseModel):
    episode_uuid: UUID
    episode_url: HttpUrl
    podcast_id: PositiveInt
    duration: PositiveInt  # Ensures duration is positive
    file_type: str = Field(..., min_length=1)
    language: str = Field(..., min_length=2, max_length=5)  # Enforces proper language code
    is_explicit: StrictBool
    publisher: str = Field(..., min_length=1)
    description_cleaned: str = Field(..., min_length=1)
    title_cleaned: str = Field(..., min_length=1)
    readability: PositiveInt
    description_selected: PositiveInt
    advanced_popularity: PositiveFloat
    index_status: PositiveInt
    record_hash: str = Field(..., min_length=1)
    publish_date: Optional[PastDatetime]  # Allows null values for past dates
