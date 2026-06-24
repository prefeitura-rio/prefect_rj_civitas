from google.cloud import bigquery
from typing import Literal
from pydantic import BaseModel, Field
from typing import Optional, List, Literal

class LLMGeoSchema(BaseModel):
    is_relevant: bool = Field(description="True se for um relato sobre ocorrência de um ou mais crimes. False se não contiver nenhum relato de um ou mais crimes")
    locations: List[str] = Field(description="Todas as localizações geográficas encontradas no texto, como localizações públicas (praça, estação de metrô, hospital, etc), ruas, praças, esquinas, bairros e cidades. Se não houver, retorne uma lista vazia.")
    main_location: Optional[str] = Field(description=(
        "Localização mais específica e detalhada onde ocorreu a principal atividade criminosa do texto. "
        "IGNORE COMPLETAMENTE rodovias, estradas ou BRs (ex: BR-101, Rodovia Washington Luís). Elas não devem ser consideradas. "
        "Ordem de prioridade da maior granularidade para a menor: esquina, estabelecimento/praça, travessa, rua, bairro, avenida, cidade. "
        "Você deve complementar com informações de apoio. Exemplo: se o texto citar 'rua X, no bairro Y', retorne 'rua X, Y'. "
        "Se não houver nenhuma localidade válida ou apenas rodovias forem citadas, retorne uma string vazia."
        )
    )

def get_source_schema(source: Literal["news", "press", "whatsapp", "radio.medias", "television", "twitter"]):
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
        "whatsapp": [
            bigquery.SchemaField(name="id", field_type="STRING", mode="REQUIRED"),
            bigquery.SchemaField(name="chat_id", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="datetime", field_type="TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(name="text", field_type="STRING", mode="NULLABLE"),
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
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
            bigquery.SchemaField(name="tags", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="is_relevant", field_type="BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField(name="locations", field_type="STRING", mode="REPEATED"),
            bigquery.SchemaField(name="main_location", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="main_location_full_address", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="city", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="latitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="longitude", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="timestamp_insercao", field_type="timestamp", mode="NULLABLE"),
        ],
    }

    return schemas[source]

def get_source_text_fields(source: Literal["news", "press", "whatsapp", "radio.medias", "television", "twitter"]):
    text_fields = {
        "news": ["c_title_search", "c_subtitle_search", "text"],
        "press": ["c_title_search", "text"],
        "whatsapp": ["text"],
        "radio.medias": ["transcript"],
        "television": ["transcript"],
        "twitter": ["text"]
    }
    return text_fields[source]

def get_source_parameters(source: Literal["news", "press", "whatsapp", "radio.medias", "television", "twitter"]):
    general_parameters = {
        "country": "BR",
        "region": "RJ",
        "tags": ["segurança"],
        "sortOrder": "desc",
        "sortField": "datetime",
    }
    
    parameters = {
        "news": {},
        "press": {},
        "whatsapp": {
            "type_label": "chat",
            "spam": "false",
            # não funciona   "is_news_related": "true",   
            # não funciona   "is_potencially_misleading": "false"
        },
        "radio.medias": {},
        "television": {},
        "twitter": {}
    }

    return general_parameters | parameters[source]