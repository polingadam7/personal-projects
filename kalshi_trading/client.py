import base64
import os
import time
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

load_dotenv()

LIVE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"


class KalshiClient:
    def __init__(self):
        self.api_key_id = os.getenv("KALSHI_API_KEY_ID")
        self.demo = os.getenv("KALSHI_DEMO", "true").lower() == "true"
        self.base_url = DEMO_BASE_URL if self.demo else LIVE_BASE_URL

        # Support two key-loading strategies:
        #   1. KALSHI_PRIVATE_KEY  — full PEM text stored as an environment variable
        #      (preferred for cloud deployments like Railway where no file system key exists)
        #   2. KALSHI_PRIVATE_KEY_PATH — path to a PEM file on disk (default local behaviour)
        pem_text = os.getenv("KALSHI_PRIVATE_KEY")
        if pem_text:
            # Env-var may have literal "\n" instead of real newlines (common in Railway secrets)
            pem_bytes = pem_text.replace("\\n", "\n").encode("utf-8")
            self.private_key = serialization.load_pem_private_key(pem_bytes, password=None)
        else:
            key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "keys/kalshi_private_key.pem")
            key_file = Path(key_path) if Path(key_path).is_absolute() else Path(__file__).parent / key_path
            with open(key_file, "rb") as f:
                self.private_key = serialization.load_pem_private_key(f.read(), password=None)

        print(f"Kalshi client initialized ({'DEMO' if self.demo else 'LIVE'})")

    def _sign(self, method: str, path: str) -> dict:
        timestamp_ms = str(int(time.time() * 1000))
        # Signing requires the full path: /trade-api/v2/...
        api_prefix = "/trade-api/v2"
        full_path = (api_prefix + path).split("?")[0]
        message = (timestamp_ms + method.upper() + full_path).encode("utf-8")
        signature = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        }

    def _request(self, method: str, path: str, params: dict = None, body: dict = None, retries: int = 3) -> dict:
        url = self.base_url + path
        headers = self._sign(method, path)
        if body is not None:
            headers["Content-Type"] = "application/json"
        for attempt in range(retries):
            resp = requests.request(method, url, headers=headers, params=params, json=body)
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"Rate limited — retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return resp.json()

    def _get(self, path: str, params: dict = None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, body=body)

    def _delete(self, path: str) -> dict:
        return self._request("DELETE", path)

    # --- Market data (public, but we sign anyway for simplicity) ---

    def get_markets(self, event_ticker: str = None, series_ticker: str = None, status: str = "open", limit: int = 100) -> list:
        params = {"status": status, "limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        if series_ticker:
            params["series_ticker"] = series_ticker
        return self._get("/markets", params).get("markets", [])

    def get_market(self, ticker: str) -> dict:
        return self._get(f"/markets/{ticker}").get("market", {})

    def get_orderbook(self, ticker: str, depth: int = 10) -> dict:
        return self._get(f"/markets/{ticker}/orderbook", {"depth": depth}).get("orderbook", {})

    def get_market_history(self, ticker: str, limit: int = 100) -> list:
        return self._get(f"/markets/{ticker}/history", {"limit": limit}).get("history", [])

    # --- Account ---

    def get_balance(self) -> dict:
        return self._get("/portfolio/balance")

    def get_positions(self) -> list:
        return self._get("/portfolio/positions").get("market_positions", [])

    def get_orders(self, status: str = None) -> list:
        params = {"status": status} if status else {}
        return self._get("/portfolio/orders", params).get("orders", [])

    # --- Trading ---

    def place_order(self, ticker: str, side: str, count: int, order_type: str = "limit", yes_price: int = None, no_price: int = None) -> dict:
        """
        side: 'yes' or 'no'
        prices are in cents (1-99)
        order_type: 'limit' or 'market'
        """
        body = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": count,
            "type": order_type,
        }
        if order_type == "limit":
            if yes_price is not None:
                body["yes_price"] = yes_price
            if no_price is not None:
                body["no_price"] = no_price
        return self._post("/portfolio/orders", body)

    def cancel_order(self, order_id: str) -> dict:
        return self._delete(f"/portfolio/orders/{order_id}")
