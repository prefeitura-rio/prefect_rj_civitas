from pydantic import BaseModel
from typing import Literal

class News(BaseModel):
    c_int_chars: int
    c_int_reach: int
    c_int_words: int
    c_metadata_id: str
    c_modified_date: str
    ca_image_urls: list[str]
    chat_id: str
    country: str
    datetime: str
    datetime_search: str
    id: str
    impressions: float
    lang: str
    price: float   
    region: str
    source: str
    text: str
    c_feed_id: str
    c_image: str
    c_portal_id: str
    c_processed_at: str
    c_state: str
    c_subtitle_search: str
    c_title_search: str
    c_url: str
    ca_authors: str
    

general_parameters = {
    "country": "BR",
    "region": "RJ",
    "tags": ["segurança"],
    "sortOrder": "desc",
    "sortField": "datetime",
}

def get_source_schema(source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]):
    schemas = {
        "news": News,
    }

    return schemas[source]

def get_source_parameters(source: Literal["whatsapp", "news", "press", "radio.medias", "television", "twitter"]):
    parameters = {
        "news": {}
    }

    return general_parameters | parameters[source]