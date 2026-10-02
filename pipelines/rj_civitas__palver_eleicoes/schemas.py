# -*- coding: utf-8 -*-
from google.cloud import bigquery
from typing import Literal
from pydantic import BaseModel, Field
from typing import Literal

class LLMGeoSchema(BaseModel):
    sensacionalista: bool = Field(description="True se o texto possuir um tom sensacionalista: adjetivos exagerados, apelo à emoção intensa, textos em caixa alta, chamadas à ação. Caso contrário, o valor deve ser False")
    ataque_urnas: bool = Field(description="True se o texto contiver críticas ao método de votação brasileiro (urnas eletrônicas), levantar suspeitas quanto à sua confiabilidade ou incitar ataques às urnas ou à democracia brasileira. Do contrário deve ser False")
    difamacao: bool = Field(description="True se o texto for um ataque pessoal ou político a algum indivíduo ou grupo, com objetivo de aqtacar a imagem pública do indivíduo ou grupo. Do contrário deve ser False")

def get_source_schema(source: Literal["news", "press", "whatsapp", "radio.medias", "television", "twitter", "telegram"]):
    schemas = {
        "news": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime_search", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_title_search", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_subtitle_search", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_url", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="ca_authors", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "press": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="c_processed_at", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_title_search", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="media_path", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "whatsapp": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="urls", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="text_sentiment", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="is_news_related", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="news_related_score", field_type="FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(name="spam", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="spam_score", field_type="FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(name="is_potentially_fraud", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="fraud_score", field_type="FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(name="is_potentially_misleading", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="misleading_score", field_type="FLOAT", mode="NULLABLE"),
            bigquery.SchemaField(name="sender_behaviour", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "telegram": [
                    bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
                    bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="transcript", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="ca_urls", field_type="STRING", mode="REPEATED"),
                    bigquery.SchemaField(name="is_news_related", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="news_related_score", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="spam", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="spam_score", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="is_potentially_fraud", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="fraud_score", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="is_potentially_misleading", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="misleading_score", field_type="FLOAT", mode="NULLABLE"),
                    bigquery.SchemaField(name="sender_behaviour", field_type="STRING", mode="NULLABLE"),
                    bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
                    bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "radio.medias": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript_sentiment", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_radio_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_radio_name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_program_title", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="media_path", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "television": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="transcript_sentiment", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_channel_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_channel_name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="program_title", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="program_category", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="media_path", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "twitter": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_url", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_username", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="c_user_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="hashtags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="sensacionalista", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="ataque_urnas", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="difamacao", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
    }

    return schemas[source]

def get_source_text_fields(source: Literal["news", "press", "whatsapp", "radio.medias", "television", "twitter", "telegram"]):
    text_fields = {
        "news": ["c_title_search", "c_subtitle_search", "text"],
        "press": ["c_title_search", "text"],
        "whatsapp": ["text", "transcript"],
        "telegram": ["text", "transcript"],
        "radio.medias": ["transcript"],
        "television": ["transcript"],
        "twitter": ["text", "transcript"]
    }
    return text_fields[source]