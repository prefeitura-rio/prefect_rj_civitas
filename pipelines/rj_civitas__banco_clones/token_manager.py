# -*- coding: utf-8 -*-
import base64
import json
import requests
import threading
import time

from typing import Optional


class TokenManager():
    def __init__(
        self,
        api_url: str,
        api_client_id: str,
        api_client_secret: str,
        logger
    ):
        self._api_url = api_url
        self._api_client_id = api_client_id
        self._api_client_secret = api_client_secret
        self._logger = logger
        self._lock = threading.Lock()
        self.api_token: Optional[str] = None
        self._expires_at: float = 0.0

    def is_expired(self) -> bool:
        if not self.api_token:
            return True
        return time.time() + 30 >= self._expires_at

    def extract_expiration(self, token: str) -> float:
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
        with self._lock:
            if not self.is_expired():
                return self.api_token

            url = f"{self._api_url}/auth/login"
            body = {
                "client_id": self._api_client_id,
                "client_secret": self._api_client_secret
            }

            try:
                response = requests.post(url, json=body, timeout=10)
                response.raise_for_status()

                data = response.json()
                token = data.get("access_token")
                if not token:
                    self._logger.warning(
                        "Error while getting Gabriel API access token: "
                        "access_token field is missing or empty"
                    )
                    return None

                self.api_token = token
                self._expires_at = self.extract_expiration(token)
                return token

            except (requests.RequestException, ValueError) as error:
                self._logger.warning(
                    f"Error while getting Gabriel API access token: {error}"
                )
                return None

    def get(self) -> Optional[str]:
        if self.is_expired():
            return self.refresh()
        return self.api_token
