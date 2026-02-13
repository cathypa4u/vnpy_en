"""
KIS Shared Layer Module (Improved Version)
- Fixes:
  1. WebSocket connection state management (self._active vs self.active)
  2. Subscription timing issue (race condition)
  3. Enhanced reconnection and resubscription logic
  4. Better error handling and logging
  5. Connection event callback support
"""

import os
import json
import time
import threading
import logging
import tempfile
import base64
import sys
from collections import defaultdict
from typing import Dict, Set, Any, List, Optional, Callable
from pathlib import Path
from queue import Empty

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# vn.py Libraries
from vnpy.trader.utility import get_folder_path
from vnpy_rest.rest_client import RestClient, Request
from vnpy_websocket.websocket_client import WebsocketClient

# 암호화 라이브러리 (선택적)
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    logging.warning("Crypto library not found. Decryption disabled.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("KIS_SHARED")

# =============================================================================
# 상수 정의
# =============================================================================
# KIS API 서버 URL (공식 문서 기준)
KIS_REAL_REST_URL = "https://openapi.koreainvestment.com:9443"
KIS_REAL_WS_URL = "ws://ops.koreainvestment.com:21000"  # 실전 WebSocket
KIS_DEMO_REST_URL = "https://openapivts.koreainvestment.com:29443"
KIS_DEMO_WS_URL = "ws://ops.koreainvestment.com:31000"  # 모의 WebSocket

# 토큰 저장 경로
TRADER_DIR = Path.home().joinpath(".vntrader")
if not TRADER_DIR.exists():
    TRADER_DIR.mkdir()
TOKEN_FILE_REAL = TRADER_DIR.joinpath("kis_token_real.json")
TOKEN_FILE_DEMO = TRADER_DIR.joinpath("kis_token_demo.json")

# 재시도 설정
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY_BASE = 1.0  # 초

# =============================================================================
# Utility Classes
# =============================================================================
class AtomicFileAdapter:
    """원자적 파일 쓰기를 지원하는 어댑터"""
    
    @staticmethod
    def save_json(filepath: Path, data: dict):
        dir_name = filepath.parent
        if not dir_name.exists():
            dir_name.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_name, encoding="utf-8") as tmp:
            json.dump(data, tmp, indent=4, ensure_ascii=False)
            temp_name = tmp.name
        try:
            os.replace(temp_name, filepath)
        except Exception as e:
            logger.error(f"Failed to save file {filepath}: {e}")
            if os.path.exists(temp_name):
                os.remove(temp_name)

    @staticmethod
    def load_json(filepath: Path) -> dict:
        if not filepath.exists():
            return {}
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load file {filepath}: {e}")
            return {}


class RateLimiter:
    """API 호출 속도 제한기"""
    
    def __init__(self, limit_per_sec: int = 10):
        self.interval = 1.0 / limit_per_sec
        self.last_req_time = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.time()
            diff = now - self.last_req_time
            if diff < self.interval:
                time.sleep(self.interval - diff)
            self.last_req_time = time.time()


class KisCipher:
    """KIS 데이터 복호화 유틸리티"""
    
    @staticmethod
    def decrypt(iv: str, key: str, encrypted_data: str) -> str:
        if not HAS_CRYPTO:
            return ""
        try:
            cipher = AES.new(
                key.encode('utf-8')[:32],
                AES.MODE_CBC,
                iv.encode('utf-8')
            )
            decrypted = cipher.decrypt(base64.b64decode(encrypted_data))
            return unpad(decrypted, AES.block_size).decode('utf-8')
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return ""


# =============================================================================
# Auth Manager
# =============================================================================
class KisAuthManager:
    """KIS API 인증 관리자"""
    
    def __init__(self, app_key: str, app_secret: str, is_real: bool = True):
        self.app_key = app_key
        self.app_secret = app_secret
        self.is_real = is_real
        self.base_url = KIS_REAL_REST_URL if is_real else KIS_DEMO_REST_URL
        self.token_file = TOKEN_FILE_REAL if is_real else TOKEN_FILE_DEMO
        self.access_token = None
        self.token_expired_at = None
        self.approval_key = None
        self._lock = threading.RLock()
        self.hash_map = {}
        self._load_token()

    @staticmethod
    def get_base_url(is_real: bool = True) -> str:
        return KIS_REAL_REST_URL if is_real else KIS_DEMO_REST_URL

    def _load_token(self):
        """저장된 토큰 로드"""
        data = AtomicFileAdapter.load_json(self.token_file)
        
        # [수정] 파일에 저장된 AppKey와 현재 AppKey가 일치하는지 확인
        saved_app_key = data.get("app_key", "")
        
        if saved_app_key != self.app_key:
            # 키가 다르면 저장된 정보 폐기 (새로 발급 유도)
            self.access_token = None
            self.approval_key = None
            self.token_expired_at = None
            return
                
        self.access_token = data.get("access_token")
        self.approval_key = data.get("approval_key")
        try:
            from datetime import datetime
            expired_str = data.get("expired_at")
            if expired_str:
                self.token_expired_at = datetime.fromisoformat(expired_str)
        except Exception:
            self.token_expired_at = None

    def get_token(self) -> str:
        """유효한 접근 토큰 반환 (필요시 갱신)"""
        with self._lock:
            from datetime import datetime
            if not self.access_token:
                self._issue_token()
            elif self.token_expired_at:
                remaining = (self.token_expired_at - datetime.now()).total_seconds()
                if remaining < 600:  # 10분 미만 남으면 갱신
                    self._issue_token()
            return self.access_token

    def get_approval_key(self) -> str:
        """WebSocket 인증용 승인키 반환"""
        with self._lock:
            if not self.approval_key:
                self._issue_approval_key()
            return self.approval_key

    def _issue_token(self):
        """접근 토큰 발급 (자동 Fallback 적용)"""
        if not self.app_key or not self.app_secret:
            return

        # 1. 시도할 URL 목록 정의 (우선순위: tokenP -> token)
        # tokenP: 개인 고객용 (Client Credentials) / token: 일반/법인 등
        endpoints = ["/oauth2/tokenP", "/oauth2/token"]
        
        last_error = None
        
        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            headers = {"content-type": "application/json"}
            body = {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret
            }

            try:
                res = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
                res.raise_for_status()
                
                data = res.json()
                self.access_token = data.get("access_token")
                
                # 만료 시간 계산
                expires_in = int(data.get("expires_in", 86400))
                from datetime import datetime, timedelta
                self.token_expired_at = datetime.now() + timedelta(seconds=expires_in - 60)
                
                self._save_auth()
                logger.info(f"Token issued successfully using {endpoint}")
                return  # 성공하면 종료

            except Exception as e:
                last_error = e
                logger.warning(f"Token issuance failed with {endpoint}: {e}")
                # 다음 endpoint 시도

        # 모든 시도 실패 시
        logger.error("All token issuance attempts failed.")
        if last_error:
            logger.error(f"Last error: {last_error}")

    def _issue_approval_key(self):
        """WebSocket 승인키 발급"""
        try:
            res = requests.post(
                f"{self.base_url}/oauth2/Approval",
                json={
                    "grant_type": "client_credentials",
                    "appkey": self.app_key,
                    "secretkey": self.app_secret
                },
                timeout=10
            )
            res.raise_for_status()
            self.approval_key = res.json()["approval_key"]
            self._save_auth()
            logger.info("Approval key issued successfully")
        except Exception as e:
            logger.error(f"Approval key issuance failed: {e}")
            raise

    def _save_auth(self):
        """인증 정보 저장"""
        AtomicFileAdapter.save_json(self.token_file, {
            "access_token": self.access_token,
            "expired_at": self.token_expired_at.isoformat() if self.token_expired_at else "",
            "approval_key": self.approval_key
        })

    def get_hashkey(self, headers: dict, body_json: str) -> str:
        """주문용 해시키 생성"""
        with self._lock:
            if body_json in self.hash_map:
                return self.hash_map[body_json]
        try:
            res = requests.post(
                f"{self.base_url}/uapi/hashkey",
                headers=headers,
                data=body_json,
                timeout=5
            )
            if res.status_code == 200:
                h = res.json().get("HASH")
                if h:
                    with self._lock:
                        self.hash_map[body_json] = h
                    return h
        except Exception as e:
            logger.error(f"Hashkey generation failed: {e}")
        return ""


# =============================================================================
# Session Manager (REST API)
# =============================================================================
class KisSessionManager(RestClient):
    """KIS REST API 세션 관리자"""
    
    def __init__(self, auth_manager: KisAuthManager, is_real: bool, req_limit: int = 10):
        super().__init__()
        self.auth = auth_manager
        self.is_real = is_real
        self.limiter = RateLimiter(limit_per_sec=req_limit)
        
        # Base Init
        base_url = KIS_REAL_REST_URL if is_real else KIS_DEMO_REST_URL
        self.init(url_base=base_url)
        self.start()

    def start(self):
        self._active = True
        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self) -> None:
        """세션 실행 루프 (Retry Adapter 포함)"""
        try:
            session = requests.session()
            retries = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)

            while self._active:
                try:
                    request = self.queue.get(timeout=1)
                    try:
                        self.process_request(request, session)
                    finally:
                        self.queue.task_done()
                except Empty:
                    pass
        except Exception:
            exc, value, tb = sys.exc_info()
            if exc and value and tb:
                self.on_error(exc, value, tb, None)

    def sign(self, request: Request) -> Request:
        """요청 서명 (헤더 추가)"""
        self.limiter.wait()  # Rate Limit 적용
        
        token = self.auth.get_token()
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.auth.app_key,
            "appsecret": self.auth.app_secret,
            "tr_id": request.extra.get("tr_id", ""),
            "custtype": "P"
        }

        if request.method == "POST" and request.data:
            body_json = json.dumps(request.data, separators=(',', ':'))
            if "hashkey" not in headers:
                h_key = self.auth.get_hashkey(headers, body_json)
                if h_key:
                    headers["hashkey"] = h_key
            request.data = body_json

        request.headers = headers
        return request


# =============================================================================
# WebSocket Dispatcher (Improved)
# =============================================================================
class KisWsDispatcher(WebsocketClient):
    """
    KIS WebSocket 디스패처 (개선 버전)
    - 연결 상태 관리 개선
    - 구독 타이밍 문제 해결
    - 재연결 및 재구독 로직 강화
    """
    
    def __init__(self, auth_manager: KisAuthManager, is_real: bool):
        super().__init__()
        self.auth = auth_manager
        self.is_real = is_real
        self.url = KIS_REAL_WS_URL if is_real else KIS_DEMO_WS_URL
        
        # Routing & Subscription Maps
        self.routing_map: Dict[tuple, Set] = defaultdict(set)  # (tr_id, key) -> Set[Gateway]
        self.account_map: Dict[str, Any] = {}  # account -> Gateway
        self.ref_counts: Dict[tuple, int] = defaultdict(int)
        self.pending_subscriptions: List[tuple] = []  # 연결 전 구독 대기열
        
        # Encryption Keys
        self.aes_keys: Dict[str, dict] = {}
        
        # Thread Safety
        self._lock = threading.RLock()
        
        # Connection State
        self._is_connected = False
        self._connection_event = threading.Event()
        self._reconnect_count = 0
        
        # Callbacks
        self._on_connected_callbacks: List[Callable] = []
        self._on_disconnected_callbacks: List[Callable] = []

    def start_ws(self):
        """WebSocket 연결 시작"""
        logger.info(f"[{'REAL' if self.is_real else 'DEMO'}] Starting WebSocket connection to {self.url}")
        self.init(self.url, ping_interval=30)
        self.start()

    def start(self):
        """WebSocket 스레드 시작"""
        self._active = True
        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def wait_connected(self, timeout: float = 10.0) -> bool:
        """
        연결 완료 대기
        
        Args:
            timeout: 최대 대기 시간 (초)
            
        Returns:
            연결 성공 여부
        """
        return self._connection_event.wait(timeout=timeout)

    def is_connected(self) -> bool:
        """현재 연결 상태 반환"""
        return self._is_connected and self._active

    def add_connected_callback(self, callback: Callable):
        """연결 완료 콜백 등록"""
        self._on_connected_callbacks.append(callback)

    def add_disconnected_callback(self, callback: Callable):
        """연결 해제 콜백 등록"""
        self._on_disconnected_callbacks.append(callback)

    def on_connected(self):
        """연결 완료 핸들러"""
        logger.info(f"[{'REAL' if self.is_real else 'DEMO'}] WebSocket Connected")
        
        with self._lock:
            self._is_connected = True
            self._connection_event.set()
            self._reconnect_count = 0
        
        # 재구독 실행
        self._resubscribe_all()
        
        # 대기 중인 구독 처리
        self._process_pending_subscriptions()
        
        # 콜백 실행
        for callback in self._on_connected_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Connected callback error: {e}")

    def on_disconnected(self):
        """연결 해제 핸들러"""
        logger.warning(f"[{'REAL' if self.is_real else 'DEMO'}] WebSocket Disconnected")
        
        with self._lock:
            self._is_connected = False
            self._connection_event.clear()
            self._reconnect_count += 1
        
        # 콜백 실행
        for callback in self._on_disconnected_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error(f"Disconnected callback error: {e}")

    def on_error(self, exception_type, exception_value, tb, request=None):
        """에러 핸들러"""
        logger.error(f"[{'REAL' if self.is_real else 'DEMO'}] WebSocket Error: {exception_value}")

    # -------------------------------------------------------------------------
    # Message Handling
    # -------------------------------------------------------------------------
    def on_message(self, packet):
        """
        메시지 수신 핸들러
        - JSON과 Raw Text 메시지 구분 처리
        """
        try:
            # 1. Byte -> String 변환
            if isinstance(packet, bytes):
                packet = packet.decode('utf-8')

            # 2. String 처리
            if isinstance(packet, str):
                packet = packet.strip()
                if not packet:
                    return
                
                # (A) JSON 메시지 (PINGPONG, 구독응답 등)
                if packet.startswith('{'):
                    try:
                        json_data = json.loads(packet)
                        self._handle_json(json_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}, data: {packet[:100]}")
                
                # (B) Raw Text 메시지 (실시간 시세)
                # 형식: 암호화여부|TR_ID|데이터건수|데이터
                elif packet[0] in ['0', '1']:
                    self._route_realtime_data(packet)
                
                # (C) 기타 메시지
                else:
                    logger.debug(f"Unknown packet format: {packet[:50]}")
                    
            # 3. Dict 처리 (이미 파싱된 경우)
            elif isinstance(packet, dict):
                self._handle_json(packet)

        except Exception as e:
            logger.error(f"Message handling error: {e}")

    def on_packet(self, packet: Any):
        """패킷 처리 (호환성 유지)"""
        try:
            if isinstance(packet, dict):
                self._handle_json(packet)
            else:
                self._route_realtime_data(str(packet))
        except Exception as e:
            logger.error(f"Packet handling error: {e}")

    # -------------------------------------------------------------------------
    # Subscription Management
    # -------------------------------------------------------------------------
    def subscribe(self, gateway, tr_id: str, tr_key: str):
        """
        실시간 데이터 구독
        
        Args:
            gateway: 콜백을 받을 게이트웨이 객체
            tr_id: TR ID (예: H0STCNT0)
            tr_key: 구독 키 (예: 종목코드)
        """
        with self._lock:
            k = (tr_id, tr_key)
            self.routing_map[k].add(gateway)
            self.ref_counts[k] += 1
            
            logger.info(f"[SUBSCRIBE] tr_id={tr_id}, key={tr_key}, "
                       f"ref_count={self.ref_counts[k]}, connected={self._is_connected}")
            
            # 첫 번째 구독자일 때만 서버에 구독 요청
            if self.ref_counts[k] == 1:
                if self._is_connected:
                    self._send_subscribe_packet(tr_id, tr_key)
                else:
                    # 연결 전이면 대기열에 추가
                    self.pending_subscriptions.append((tr_id, tr_key))
                    logger.info(f"[SUBSCRIBE] Queued (not connected): {tr_id}/{tr_key}")

    def unsubscribe(self, gateway, tr_id: str, tr_key: str):
        """
        실시간 데이터 구독 해제
        """
        with self._lock:
            k = (tr_id, tr_key)
            if gateway in self.routing_map[k]:
                self.routing_map[k].remove(gateway)
                self.ref_counts[k] -= 1
                
                logger.info(f"[UNSUBSCRIBE] tr_id={tr_id}, key={tr_key}, "
                           f"ref_count={self.ref_counts[k]}")
                
            if self.ref_counts[k] <= 0:
                if self._is_connected:
                    self._send_unsubscribe_packet(tr_id, tr_key)
                # 맵에서 제거
                if k in self.ref_counts:
                    del self.ref_counts[k]
                if k in self.routing_map:
                    del self.routing_map[k]

    def register_gateway(self, gateway, account_list: List[str]):
        """게이트웨이 및 계좌 등록 (체결통보용)"""
        with self._lock:
            for acc in account_list:
                self.account_map[acc] = gateway
            logger.info(f"Gateway registered with accounts: {account_list}")

    def _resubscribe_all(self):
        """모든 기존 구독 재전송 (재연결 시)"""
        with self._lock:
            if not self._active or not self._is_connected:
                return
                
            count = 0
            for (tr_id, key), ref_count in list(self.ref_counts.items()):
                if ref_count > 0:
                    self._send_subscribe_packet(tr_id, key)
                    count += 1
                    time.sleep(0.05)  # 서버 부하 방지
                    
            logger.info(f"[RESUBSCRIBE] Restored {count} subscriptions")

    def _process_pending_subscriptions(self):
        """대기 중인 구독 처리"""
        with self._lock:
            if not self.pending_subscriptions:
                return
                
            pending = self.pending_subscriptions.copy()
            self.pending_subscriptions.clear()
            
            for tr_id, tr_key in pending:
                self._send_subscribe_packet(tr_id, tr_key)
                time.sleep(0.05)
                
            logger.info(f"[PENDING] Processed {len(pending)} pending subscriptions")

    def _send_subscribe_packet(self, tr_id: str, tr_key: str):
        """구독 패킷 전송"""
        self._send_packet("1", tr_id, tr_key)
        logger.debug(f"[SEND] Subscribe: {tr_id}/{tr_key}")

    def _send_unsubscribe_packet(self, tr_id: str, tr_key: str):
        """구독 해제 패킷 전송"""
        self._send_packet("2", tr_id, tr_key)
        logger.debug(f"[SEND] Unsubscribe: {tr_id}/{tr_key}")

    def _send_packet(self, tr_type: str, tr_id: str, tr_key: str):
        """WebSocket 패킷 전송"""
        packet = {
            "header": {
                "approval_key": self.auth.get_approval_key(),
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": tr_key
                }
            }
        }
        self.send_packet(packet)

    # -------------------------------------------------------------------------
    # Data Routing
    # -------------------------------------------------------------------------
    def _route_realtime_data(self, msg: str):
        """
        실시간 데이터 라우팅
        
        메시지 형식: 암호화여부|TR_ID|데이터건수|데이터
        """
        tokens = msg.split('|')
        if len(tokens) < 4:
            return
            
        is_encrypted = (tokens[0] == '1')
        tr_id = tokens[1]
        data_count = tokens[2]
        raw_data = tokens[3]
        
        # [Tick/호가] TR_ID로 판별
        if self._is_tick_tr(tr_id):
            # 첫 번째 필드가 종목코드
            symbol = raw_data.split('^')[0]
            
            with self._lock:
                targets = self.routing_map.get((tr_id, symbol))
                
            if targets:
                for gw in targets:
                    if hasattr(gw, "on_ws_data_push"):
                        gw.on_ws_data_push("tick", (tr_id, msg))
            else:
                logger.debug(f"[ROUTE] No target for {tr_id}/{symbol}")
        
        # [체결통보] CN 포함 또는 특정 TR
        elif "CNI" in tr_id or tr_id in ["HDFFF1C0", "HDFFF2C0"]:
            payload = {
                "tr_id": tr_id,
                "is_encrypted": is_encrypted,
                "data": raw_data,
                "raw_msg": msg
            }
            
            with self._lock:
                unique_gws = set(self.account_map.values())
                
            for gw in unique_gws:
                if hasattr(gw, "on_ws_data_push"):
                    gw.on_ws_data_push("order", payload)

    def _is_tick_tr(self, tr_id: str) -> bool:
        """시세/호가 TR 여부 판별"""
        # 국내주식: H0ST*, H0NX*, H0UN*
        # 국내선물옵션: H0IF*, H0IO*
        # 국내채권: H0BJ*
        # 국내업종: H0UP*
        # 야간선물: H0MF*, H0EU*, ECEU*
        # 해외주식: HDFS*
        # 해외선물: HDFFF*
        prefixes = (
            "H0ST", "H0NX", "H0UN",  # 국내주식
            "H0IF", "H0IO",          # 국내선물옵션
            "H0BJ",                   # 국내채권
            "H0UP",                   # 국내업종
            "H0MF", "H0EU", "ECEU",  # 야간선물
            "HDFS",                   # 해외주식
            "HDFFF"                   # 해외선물 (tick/hoka)
        )
        return any(tr_id.startswith(p) for p in prefixes)

    def _handle_json(self, js: dict):
        """JSON 메시지 처리"""
        header = js.get("header", {})
        tr_id = header.get("tr_id", "")
        
        # PINGPONG 응답
        if tr_id == "PINGPONG":
            self.send_packet(js)
            return
            
        # 구독 응답 처리
        body = js.get("body", {})
        output = body.get("output", {})
        
        # 암호화 키 저장
        if "iv" in output and "key" in output:
            self.aes_keys[tr_id] = {
                "iv": output["iv"],
                "key": output["key"]
            }
            logger.debug(f"AES key stored for {tr_id}")
        
        # 구독 결과 로그
        rt_cd = body.get("rt_cd", "")
        msg1 = body.get("msg1", "")
        if rt_cd:
            if rt_cd == "0":
                logger.info(f"[RESPONSE] {tr_id}: Success - {msg1}")
            else:
                logger.warning(f"[RESPONSE] {tr_id}: Failed ({rt_cd}) - {msg1}")

    def decrypt_data(self, tr_id: str, iv: str, enc_data: str) -> str:
        """암호화된 데이터 복호화"""
        k = self.aes_keys.get(tr_id)
        if k:
            return KisCipher.decrypt(iv if iv else k["iv"], k["key"], enc_data)
        return ""


# =============================================================================
# Shared Facade (Singleton)
# =============================================================================
class KisShared:
    """
    KIS Shared Layer 싱글톤 파사드
    - 다중 게이트웨이 지원
    - 인증/세션/WebSocket 공유
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(KisShared, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def init(
        self,
        real_app_key: str = "",
        real_secret: str = "",
        demo_app_key: str = "",
        demo_secret: str = "",
        req_limit: int = 15
    ):
        """
        Shared Layer 초기화
        
        Args:
            real_app_key: 실전 APP Key
            real_secret: 실전 APP Secret
            demo_app_key: 모의 APP Key
            demo_secret: 모의 APP Secret
            req_limit: 초당 요청 제한
        """
        with self._lock:
            need_reinit = False
            
            if self._initialized:
                # 키 변경 시 재초기화
                current_real_key = self.auth_real.app_key if self.auth_real else ""
                current_demo_key = self.auth_demo.app_key if self.auth_demo else ""
                
                if current_real_key != real_app_key or current_demo_key != demo_app_key:
                    need_reinit = True
                    self.stop()

            if not self._initialized or need_reinit:
                # Auth Managers
                self.auth_real = KisAuthManager(real_app_key, real_secret, True) if real_app_key else None
                self.auth_demo = KisAuthManager(demo_app_key, demo_secret, False) if demo_app_key else None
                
                # Session Managers (REST)
                self.session_real = KisSessionManager(self.auth_real, True, req_limit) if self.auth_real else None
                self.session_demo = KisSessionManager(self.auth_demo, False, req_limit) if self.auth_demo else None
                
                # WebSocket Dispatchers
                self.ws_real = KisWsDispatcher(self.auth_real, True) if self.auth_real else None
                self.ws_demo = KisWsDispatcher(self.auth_demo, False) if self.auth_demo else None
                
                self._initialized = True
                logger.info(f"KIS Shared Layer Initialized (Req Limit: {req_limit})")

    def get_session(self, is_real: bool) -> Optional[KisSessionManager]:
        """REST 세션 반환"""
        return self.session_real if is_real else self.session_demo

    def get_ws(self, is_real: bool) -> Optional[KisWsDispatcher]:
        """WebSocket 디스패처 반환"""
        return self.ws_real if is_real else self.ws_demo

    def get_auth(self, is_real: bool) -> Optional[KisAuthManager]:
        """인증 관리자 반환"""
        return self.auth_real if is_real else self.auth_demo

    def start_ws(self, wait_connected: bool = False, timeout: float = 10.0) -> bool:
        """
        WebSocket 연결 시작
        
        Args:
            wait_connected: 연결 완료까지 대기 여부
            timeout: 대기 시간 (초)
            
        Returns:
            wait_connected=True일 때 연결 성공 여부
        """
        success = True
        
        if self.ws_real:
            self.ws_real.start_ws()
            if wait_connected:
                if not self.ws_real.wait_connected(timeout):
                    logger.warning("REAL WebSocket connection timeout")
                    success = False
                    
        if self.ws_demo:
            self.ws_demo.start_ws()
            if wait_connected:
                if not self.ws_demo.wait_connected(timeout):
                    logger.warning("DEMO WebSocket connection timeout")
                    success = False
                    
        return success

    def stop(self):
        """모든 연결 종료"""
        logger.info("Stopping KIS Shared Layer...")
        
        if hasattr(self, 'session_real') and self.session_real:
            self.session_real.stop()
        if hasattr(self, 'session_demo') and self.session_demo:
            self.session_demo.stop()
        if hasattr(self, 'ws_real') and self.ws_real:
            self.ws_real.stop()
        if hasattr(self, 'ws_demo') and self.ws_demo:
            self.ws_demo.stop()
            
        self._initialized = False

    def decrypt(self, is_real: bool, tr_id: str, iv: str, data: str) -> str:
        """암호화 데이터 복호화"""
        ws = self.get_ws(is_real)
        return ws.decrypt_data(tr_id, iv, data) if ws else ""

    def is_connected(self, is_real: bool) -> bool:
        """WebSocket 연결 상태 확인"""
        ws = self.get_ws(is_real)
        return ws.is_connected() if ws else False
