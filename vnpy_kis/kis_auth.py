"""
KIS Auth Manager (Fixed Version)
FileName: kis_auth.py
- Fix: Resolved TypeError in file saving (Path object concatenation)
- Update: Added robust logging for file I/O errors
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
                # 서버 타입(REAL/DEMO)이 일치하는지 확인
                if token_info.get("server") == server:
                    return token_info["token"]

            # 2. Refresh Token
            return self._request_new_token(app_key, app_secret, server)

    def _request_new_token(self, app_key, app_secret, server):
        # 서버 이름 정규화
        is_demo = (server == "DEMO" or server == "VIRTUAL")
        domain = "https://openapivts.koreainvestment.com:29443" if is_demo else "https://openapi.koreainvestment.com:9443"
        url = f"{domain}/oauth2/tokenP"
        
        if not app_key or not app_secret:
            raise Exception("Token Error: app_key 또는 app_secret이 비어있습니다.")
        
        try:
            res = requests.post(url, json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret
            }, timeout=10)
            
            if res.status_code != 200:
                error_text = res.text[:500] if res.text else "No response body"
                raise Exception(f"Token Error: HTTP {res.status_code} - {error_text}")
            
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
                print(f"[KisAuth] Token Refreshed & Saved for ...{app_key[-4:]} (Server: {server})")
                return token
            else:
                error_msg = data.get('error_description') or data.get('msg1') or str(data)
                raise Exception(f"Token Error: {error_msg}")
                
        except Exception as e:
            raise Exception(f"Connection Error during Auth: {str(e)}")

    def _request_hashkey(self, app_key, app_secret, server, data_body):
        """Generate HashKey for POST requests"""
        domain = "https://openapivts.koreainvestment.com:29443" if server == "DEMO" else "https://openapi.koreainvestment.com:9443"
        url = f"{domain}/uapi/hashkey"
        
        try:
            res = requests.post(url, json=data_body, headers={
                "content-type": "application/json; charset=utf-8",
                "appkey": app_key,
                "appsecret": app_secret
            }, timeout=5)
            return res.json().get("HASH", "")
        except Exception:
            return ""

    def check_rate_limit(self, app_key, limit_interval=0.06):
        with self.req_lock:
            last_time = self.req_timers[app_key]
            elapsed = time.time() - last_time
            if elapsed < limit_interval:
                time.sleep(limit_interval - elapsed)
            self.req_timers[app_key] = time.time()

    def get_header(self, tr_id, app_key, app_secret, server="REAL", body_data=None):
        token = self.get_token(app_key, app_secret, server)
        header = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "tr_cont": "",
            "custtype": "P",
        }
        if body_data and isinstance(body_data, dict):
            h_key = self._request_hashkey(app_key, app_secret, server, body_data)
            if h_key: header["hashkey"] = h_key
        return header

    def _save_tokens_to_file(self):
        """
        [Fix] Convert Path to string before concatenation
        """
        try:
            # get_file_path returns a Path object, causing TypeError if added to str directly
            file_path_str = str(self.file_path)
            temp_path = file_path_str + ".tmp"
            
            with open(temp_path, "w") as f:
                json.dump(self.tokens, f, indent=4)
            
            # Atomic replacement
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
            os.rename(temp_path, self.file_path)
            
        except Exception as e:
            print(f"⚠️ [KisAuth] Failed to save token file: {e}")

    def _load_tokens_from_file(self):
        if not os.path.exists(self.file_path): return
        try:
            with open(self.file_path, "r") as f:
                self.tokens = json.load(f)
        except Exception as e:
            print(f"⚠️ [KisAuth] Failed to load token file: {e}")

kis_auth = KisAuthManager()