"""
KIS Auth Manager (Advanced)
Features:
- Multi-AppKey Support: Manages tokens per AppKey to allow hybrid (Real/Paper) operations.
- Centralized Rate Limiting: Prevents QPS ban across multiple gateway instances.
- Thread-Safe: Uses Locks for token generation and file I/O.
"""

import json
import time
import threading
import os
import requests
from collections import defaultdict
from vnpy.trader.utility import get_file_path

class KisAuthManager:
    """
    Singleton for managing KIS Auth Tokens and Rate Limits per AppKey.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(KisAuthManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, "initialized"): return
        
        # Structure: {app_key: {"token": str, "expiry": float, "server": str}}
        self.tokens = {}
        # Rate Limiting: {app_key: last_request_time}
        self.req_timers = defaultdict(float)
        self.req_lock = threading.Lock()
        
        self.file_path = get_file_path("kis_tokens_storage.json")
        self._load_tokens_from_file()
        self.initialized = True

    def get_token(self, app_key, app_secret, server="REAL"):
        """
        Returns a valid token for the given AppKey. Refreshes if expired.
        """
        with self._lock:
            now = time.time()
            token_info = self.tokens.get(app_key)

            # 1. Check validity (buffer 60s)
            if token_info and now < token_info["expiry"] - 60:
                return token_info["token"]

            # 2. Refresh Token
            return self._request_new_token(app_key, app_secret, server)

    def _request_new_token(self, app_key, app_secret, server):
        domain = "https://openapivts.koreainvestment.com:29443" if server == "DEMO" else "https://openapi.koreainvestment.com:9443"
        url = f"{domain}/oauth2/tokenP"
        
        # KIS Token Request
        try:
            res = requests.post(url, json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret
            })
            data = res.json()
            
            if "access_token" in data:
                token = data["access_token"]
                expiry = time.time() + int(data.get("expires_in", 86400))
                
                self.tokens[app_key] = {
                    "token": token,
                    "expiry": expiry,
                    "server": server
                }
                self._save_tokens_to_file()
                print(f"[KisAuth] Token Refreshed for AppKey ending in ...{app_key[-4:]}")
                return token
            else:
                raise Exception(f"Token Error: {data.get('error_description', data)}")
        except Exception as e:
            raise Exception(f"Connection Error during Auth: {e}")

    def check_rate_limit(self, app_key, limit_interval=0.06):
        """
        Prevents API Ban. Default 0.06s (approx 16 requests/sec safe margin).
        KIS standard: 20 req/sec (Real), 2 req/sec (Paper - strict!)
        """
        with self.req_lock:
            last_time = self.req_timers[app_key]
            elapsed = time.time() - last_time
            if elapsed < limit_interval:
                time.sleep(limit_interval - elapsed)
            self.req_timers[app_key] = time.time()

    def _save_tokens_to_file(self):
        try:
            with open(self.file_path, "w") as f:
                json.dump(self.tokens, f)
        except Exception: pass

    def _load_tokens_from_file(self):
        if not os.path.exists(self.file_path): return
        try:
            with open(self.file_path, "r") as f:
                self.tokens = json.load(f)
        except Exception: pass

kis_auth = KisAuthManager()