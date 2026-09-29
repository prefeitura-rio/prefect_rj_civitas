# -*- coding: utf-8 -*-
import base64
import json
import requests
import time

from typing import Optional
from iplanrio.pipelines_utils.logging import log


class TokenManager():
    def __init__(self, api_url: str, api_client_id: str, api_client_secret: str):
        self.api_url = api_url
        self.api_client_id = api_client_id
        self.api_client_secret = api_client_secret
        self.api_token: Optional[str] = None
        self.expires_at: float = 0.0

    def is_expired(self) -> bool:
        if not self.api_token:
            return True
        return time.time() + 30 >= self.expires_at

    def extract_expiration(self, token: str):
        try:
            _, payload_b64, _ = token.split(".")
            # Corrige o padding do base64 se necessário
            payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
            payload_json = base64.b64decode(payload_b64).decode("utf-8")
            payload = json.loads(payload_json)
            return float(payload.get("exp", 0))
        except Exception:
            return 0.0

    def refresh(self) -> Optional[str]:
        url = f"{self.api_url}/auth/login"
        body = {
            "client_id": self.api_client_id,
            "client_secret": self.api_client_secret
        }

        try:
            response = requests.post(url, json=body, timeout=10)
            response.raise_for_status()

            data = response.json()
            token = data.get("access_token")
            if not token:
                log(f"Error while getting Gabriel API access token: access_token field is missing or empty", level="warning")
            self.api_token = token
            self.expires_at = self.extract_expiration(token)
            return token

        except (requests.RequestException, ValueError) as error:
            log(f"Error while getting Gabriel API access token: {error}", level="warning")
            return None

    def get(self):
        if self.is_expired():
            return self.refresh()
        return self.api_token