"""
KIS Gateway Module (Improved Version)
- Fixes:
  1. WebSocket connection timing (wait for connection before subscribing)
  2. Proper subscription flow with pending queue
  3. Enhanced tick merging and caching
  4. Better error handling and logging
  5. Multi-account support improvements
"""

import threading
import time
import json
import os
import queue
import logging
from collections import deque
from copy import copy
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from zoneinfo import ZoneInfo

from vnpy.event import EventEngine, Event
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.engine import MainEngine
from vnpy.trader.constant import (
    Exchange, Product, OrderType, Direction, Status, Interval, OptionType, Offset
)
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, OrderRequest, CancelRequest, SubscribeRequest,
    HistoryRequest, QuoteRequest, QuoteData
)
from vnpy.trader.database import DB_TZ
from vnpy.trader.event import EVENT_CONTRACT
from vnpy.trader.utility import get_folder_path
from vnpy_rest.rest_client import Request

# Local Imports
try:
    from kis_shared import KisShared, AtomicFileAdapter
    from kis_api_helper import KisConfig, KisApiHelper, AssetType
    from kis_parser import KisParser, get_krx_pricetick, KIS_TZ
    from kis_datafeed import KisDatafeed
    from kis_master import KisMasterManager
    from market_checker import market_state
except ImportError:
    try:
        from .kis_shared import KisShared, AtomicFileAdapter
        from .kis_api_helper import KisConfig, KisApiHelper, AssetType
        from .kis_parser import KisParser, get_krx_pricetick, KIS_TZ
        from .kis_datafeed import KisDatafeed
        from .kis_master import KisMasterManager
        from .market_checker import market_state
    except ImportError:
        from vnpy_kis.kis_shared import KisShared, AtomicFileAdapter
        from vnpy_kis.kis_api_helper import KisConfig, KisApiHelper, AssetType
        from vnpy_kis.kis_parser import KisParser, get_krx_pricetick, KIS_TZ
        from vnpy_kis.kis_datafeed import KisDatafeed
        from vnpy_kis.kis_master import KisMasterManager
        from vnpy_kis.market_checker import market_state

# 로깅 설정
logger = logging.getLogger("KIS_GATEWAY")

# =============================================================================
# Constants & Settings
# =============================================================================
KIS_DIR = get_folder_path("kis")
ACCOUNT_FILE = os.path.join(KIS_DIR, "kis_accounts.json")
INTEREST_FILE = os.path.join(KIS_DIR, "kis_interest.json")

# 계좌 설정 파일 기본값 생성
if not os.path.exists(ACCOUNT_FILE):
    default_acc = {
        "모의투자기본": {
            "server": "DEMO",
            "app_key": "",
            "app_secret": "",
            "account_no": "",
            "account_code": "01",
            "hts_id": "",
            "assets": ["KR_STOCK"]
        }
    }
    try:
        os.makedirs(os.path.dirname(ACCOUNT_FILE), exist_ok=True)
        with open(ACCOUNT_FILE, "w", encoding="utf-8") as f:
            json.dump(default_acc, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to create default account file: {e}")


def load_account_names() -> List[str]:
    """kis_accounts.json에서 계좌 별칭 목록 로드 (UI용)"""
    if os.path.exists(ACCOUNT_FILE):
        try:
            with open(ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return list(data.keys())
        except Exception as e:
            logger.error(f"Failed to load account names: {e}")
    return []

KR_STOCK_EXCHANGES = [Exchange.KRX, Exchange.SOR, Exchange.NXT]

# =============================================================================
# KIS Gateway
# =============================================================================
class KisGateway(BaseGateway):
    """
    KIS Gateway (Improved Version)
    
    Features:
    - Shared Layer Architecture for multi-gateway support
    - Async Order Processing
    - Smart Tick Merging (체결가 + 호가)
    - Dual Queue System (Tick: drop-oldest, Order: reliable)
    - Master Data Integration
    - Auto-Resubscribe on Reconnection
    """
    
    default_setting = {
        "User ID": "",  # HTS ID 입력 필드
        "사용계정": load_account_names()
    }
    
    exchanges = list(KisConfig.VN_TO_KIS_EXCHANGE.keys())
    default_name = "KIS"

    def __init__(self, event_engine: EventEngine, gateway_name: str = "KIS"):
        super().__init__(event_engine, gateway_name)

        # 1. Core Modules
        self.shared = KisShared()
        self.master_manager = KisMasterManager(base_dir=os.path.join(str(KIS_DIR), "master_data"))
        self.datafeed: Optional[KisDatafeed] = None
        self.main_engine: Optional[MainEngine] = None
        
        # 2. Account Context
        self.active = False
        self.main_account_no = ""
        self.server_type = "REAL"  # REAL or DEMO
        self.user_id = ""          # HTS ID for Notification
        self.active_assets: List[str] = []
        
        # 3. Queues (Dual Queue for Backpressure)
        self.tick_queue: deque = deque(maxlen=2000)  # Drop-oldest
        self.order_queue: queue.Queue = queue.Queue()  # Reliable
        self.ws_thread: Optional[threading.Thread] = None
        self._queue_lock = threading.Lock()

        # 4. Data Cache & Maps
        self.tick_cache: Dict[str, TickData] = {}
        self.contract_map: Dict[str, ContractData] = {}
        self.order_map: Dict[str, OrderData] = {}
        
        # 5. ID Mapping (Local <-> Broker)
        self.local_oid_map: Dict[str, str] = {}   # local_id -> kis_odno
        self.kis_oid_map: Dict[str, str] = {}     # kis_odno -> local_id
        self._order_counter = 0
        self._order_lock = threading.Lock()
        
        # 6. Quote Management (Two-sided Order)
        self.quote_count = 0
        self.quote_map: Dict[str, Tuple[str, str]] = {}  # quote_id -> (buy_id, sell_id)

        # 7. Subscribed List
        # 구조: { symbol: Exchange }  예: { "005930": Exchange.SOR }
        self.subscribed: Dict[str, Exchange] = {}
        
        # 8. Connection State
        self._ws_connected = False
        self._ws_connection_event = threading.Event()

    def set_main_engine(self, engine: MainEngine):
        """메인 엔진 설정"""
        self.main_engine = engine
        
    def _generate_order_id(self) -> str:
        """고유한 주문 ID 생성"""
        with self._order_lock:
            self._order_counter += 1
            timestamp = int(time.time() * 1000)
            return f"{timestamp}-{self._order_counter}"

    # =========================================================================
    # Connection Management
    # =========================================================================
    def connect(self, setting: dict):
        """
        Gateway 연결 진입점
        
        연결 순서:
        1. 계좌 설정 로드
        2. Shared Layer 초기화 (인증)
        3. 소비자 스레드 시작
        4. WebSocket 연결 및 대기
        5. 마스터 데이터 로드
        6. 관심종목 자동 구독
        7. 계좌 조회 및 체결통보 구독
        """
        alias = setting.get("사용계정")
        user_id_input = setting.get("User ID", "")

        if not alias:
            self.write_log("계좌가 선택되지 않았습니다.")
            return

        # 1. Load Account Config
        if not self._load_account_config(alias):
            self.write_log(f"계좌 설정 로드 실패: {alias}")
            return

        # UI에서 입력된 User ID가 있으면 우선 적용
        if user_id_input:
            self.user_id = user_id_input
            
        if not self.user_id:
            self.write_log("경고: User ID(HTS ID)가 설정되지 않아 실시간 체결통보를 수신할 수 없습니다.")

        # 2. Init Shared Layer (Authentication)
        keys = self._extract_all_keys()
        try:
            self.shared.init(
                real_app_key=keys.get("REAL_KEY", ""),
                real_secret=keys.get("REAL_SECRET", ""),
                demo_app_key=keys.get("DEMO_KEY", ""),
                demo_secret=keys.get("DEMO_SECRET", ""),
                req_limit=15
            )
        except Exception as e:
            self.write_log(f"인증 초기화 실패: {e}")
            return

        self.active = True
        
        # 3. Start Consumer Thread
        self.ws_thread = threading.Thread(target=self._process_ws_queue, daemon=True, name="KIS_WS_Consumer")
        self.ws_thread.start()

        # 4. Connect WebSocket via Shared Layer
        is_real = (self.server_type == "REAL")
        ws = self.shared.get_ws(is_real)
        
        if ws:
            # 게이트웨이 등록
            ws.register_gateway(self, [self.main_account_no])
            
            # 연결 콜백 등록
            ws.add_connected_callback(self._on_ws_connected)
            ws.add_disconnected_callback(self._on_ws_disconnected)
            
            # WebSocket 시작
            self.write_log(f"웹소켓 연결 시작 ({self.server_type})...")
            self.shared.start_ws(wait_connected=False)
            
            # 연결 대기 (최대 10초)
            if ws.wait_connected(timeout=10.0):
                self.write_log("웹소켓 연결 완료")
            else:
                self.write_log("경고: 웹소켓 연결 대기 시간 초과. 연결이 지연될 수 있습니다.")
        
        # 5. Background Tasks
        threading.Thread(target=self._load_master_data, daemon=True, name="KIS_Master_Loader").start()
        
        # 6. Load and Subscribe Interest List
        self._load_subscribed_list()
        
        # WebSocket 연결 후 구독 (연결 전이면 pending queue에 추가됨)
        self._auto_subscribe_interest()

        # 7. Initial Query
        self.query_account()
        self._subscribe_notice_tr()
        
        # 8. Init Datafeed
        auth = self.shared.get_auth(is_real)
        if auth:
            self.datafeed = KisDatafeed(auth_manager=auth, gateway=self)
            self.datafeed.init(app_key=auth.app_key, sec_key=auth.app_secret)

    def close(self):
        """Gateway 연결 종료"""
        self.write_log("Gateway 종료 중...")
        self.active = False
        
        # 소비자 스레드 종료 대기
        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=2.0)
        
        # Shared 리소스 정리 (싱글톤이므로 다른 게이트웨이가 사용 중일 수 있음)
        # self.shared.stop()  # 주의: 다중 게이트웨이 시 공유 리소스
        
        super().close()
        self.write_log("Gateway 종료 완료")

    def _on_ws_connected(self):
        """WebSocket 연결 완료 콜백"""
        self._ws_connected = True
        self._ws_connection_event.set()
        self.write_log("WebSocket 연결됨 - 실시간 데이터 수신 준비 완료")

    def _on_ws_disconnected(self):
        """WebSocket 연결 해제 콜백"""
        self._ws_connected = False
        self._ws_connection_event.clear()
        self.write_log("WebSocket 연결 해제됨 - 재연결 대기 중...")

    # =========================================================================
    # WebSocket Data Handling
    # =========================================================================
    def on_ws_data_push(self, data_type: str, data: Any):
        """
        Shared Layer로부터 데이터 수신
        
        Args:
            data_type: "tick" 또는 "order"
            data: 실제 데이터
        """
        if data_type == "tick":
            with self._queue_lock:
                self.tick_queue.append((data_type, data))
        elif data_type == "order":
            self.order_queue.put((data_type, data))

    def _process_ws_queue(self):
        """
        큐 소비자 루프
        - 주문 데이터 우선 처리 (신뢰성)
        - 시세 데이터 처리 (최신 우선)
        """
        while self.active:
            # 1. Process Orders (High Priority)
            processed_orders = 0
            while not self.order_queue.empty() and processed_orders < 10:
                try:
                    _, payload = self.order_queue.get_nowait()
                    self._handle_ws_order(payload)
                    processed_orders += 1
                except queue.Empty:
                    break
                except Exception as e:
                    logger.error(f"Order handling error: {e}")

            # 2. Process Ticks (Batch processing)
            processed_ticks = 0
            max_ticks_per_cycle = 50
            
            while processed_ticks < max_ticks_per_cycle:
                try:
                    with self._queue_lock:
                        if self.tick_queue:
                            _, payload = self.tick_queue.popleft()
                        else:
                            break
                    self._handle_ws_tick(payload)
                    processed_ticks += 1
                except Exception as e:
                    logger.error(f"Tick handling error: {e}")
                    break
            
            # 3. Sleep if no data
            if processed_orders == 0 and processed_ticks == 0:
                time.sleep(0.001)

    def _handle_ws_tick(self, payload: Tuple[str, str]):
        """시세 데이터 처리"""
        tr_id, msg = payload
        tick = KisParser.parse_tick(self.gateway_name, tr_id, msg)
        
        if not tick:
            return
        
        # Exchange 보정
        if not tick.exchange or tick.exchange == Exchange.LOCAL:
            c = self.contract_map.get(tick.symbol)
            if c:
                tick.exchange = c.exchange
            else:
                tick.exchange = Exchange.KRX  # 기본값

        self._merge_and_push_tick(tick)

    def _merge_and_push_tick(self, new_tick: TickData):
        """
        시세 데이터 병합 및 전송
        
        체결가(CNT)와 호가(ASP) 데이터를 하나의 TickData로 병합
        """
        symbol = new_tick.symbol
        if new_tick.exchange in [Exchange.KRX, Exchange.NXT, Exchange.SOR]:
            new_tick.exchange = self.subscribed.get(symbol, Exchange.KRX)
        
        # 1. 캐시 객체 가져오기 또는 초기화
        if symbol in self.tick_cache:
            cached = self.tick_cache[symbol]
        else:
            cached = TickData(
                gateway_name=self.gateway_name,
                symbol=symbol,
                exchange=new_tick.exchange,
                datetime=datetime.now(KIS_TZ)
            )
            self.tick_cache[symbol] = cached

        # 2. 체결 데이터 업데이트 (last_price > 0)
        if new_tick.last_price > 0:
            cached.volume = new_tick.volume
            cached.turnover = new_tick.turnover
            cached.open_interest = new_tick.open_interest
            cached.datetime = new_tick.datetime
            
            # OHLC 보정
            new_O = new_tick.open_price
            new_H = new_tick.high_price
            new_L = new_tick.low_price
            new_C = new_tick.last_price
            
            cached.open_price = new_O if new_O > 0 else max(cached.last_price,0)
            cached.high_price = new_H if new_H > 0 else max(cached.open_price, new_C)
            cached.low_price = new_L if new_L > 0 else min(cached.open_price, new_C)
            cached.last_price = new_C
            
            # if new_tick.open_price > 0:
            #     cached.open_price = new_tick.open_price
            # elif cached.open_price == 0:
            #     cached.open_price = new_tick.last_price
                
            # if new_tick.high_price > 0:
            #     cached.high_price = new_tick.high_price
            # else:
            #     cached.high_price = max(cached.high_price, new_tick.last_price)
                
            # if new_tick.low_price > 0:
            #     cached.low_price = new_tick.low_price
            # else:
            #     if cached.low_price > 0:
            #         cached.low_price = min(cached.low_price, new_tick.last_price)
            #     else:
            #         cached.low_price = new_tick.last_price
            
            # last_volume 전달 (있는 경우)
            if hasattr(new_tick, "last_volume"):
                setattr(cached, "last_volume", getattr(new_tick, "last_volume"))

        # 3. 호가 데이터 업데이트 (bid/ask > 0)
        if new_tick.bid_price_1 > 0 or new_tick.ask_price_1 > 0:
            cached.datetime = new_tick.datetime
            
            # 1~5호가
            for i in range(1, 6):
                bp = getattr(new_tick, f"bid_price_{i}", 0)
                ap = getattr(new_tick, f"ask_price_{i}", 0)
                bv = getattr(new_tick, f"bid_volume_{i}", 0)
                av = getattr(new_tick, f"ask_volume_{i}", 0)
                
                if bp > 0:
                    setattr(cached, f"bid_price_{i}", bp)
                if ap > 0:
                    setattr(cached, f"ask_price_{i}", ap)
                if bv > 0:
                    setattr(cached, f"bid_volume_{i}", bv)
                if av > 0:
                    setattr(cached, f"ask_volume_{i}", av)

        # 4. 병합된 데이터 전송 (last_price가 있을 때만)
        if cached.last_price > 0:
            self.on_tick(copy(cached))

    def _handle_ws_order(self, payload: dict):
        """체결/주문 통보 처리"""
        tr_id = payload.get('tr_id', '')
        raw_msg = payload.get('data', '')
        is_encrypted = payload.get('is_encrypted', False)
        is_real = (self.server_type == "REAL")
        
        # 복호화 (필요시)
        if is_encrypted:
            decrypted = self.shared.decrypt(is_real, tr_id, "", raw_msg)
            if decrypted:
                payload['data'] = decrypted

        # 파싱
        order, trade = KisParser.parse_order_notice(self.gateway_name, payload)
        
        if order:
            # Local ID 매핑
            local_id = self.kis_oid_map.get(order.orderid, order.orderid)
            order.orderid = local_id
            
            # Exchange 보정
            self._fix_exchange_info(order)
            
            # 상태 보정
            if order.status == Status.NOTTRADED and order.traded > 0:
                if order.volume == order.traded:
                    order.status = Status.ALLTRADED
                else:
                    order.status = Status.PARTTRADED
            
            self.order_map[local_id] = order
            self.on_order(order)
            
        if trade:
            # Local ID 매핑
            local_id = self.kis_oid_map.get(trade.orderid, trade.orderid)
            trade.orderid = local_id
            trade.tradeid = f"{trade.orderid}_{int(time.time() * 1000000)}"
            
            # Exchange 보정
            self._fix_exchange_info(trade)
            
            self.on_trade(trade)
            
            # 체결 후 잔고 갱신
            self.query_account()

    # =========================================================================
    # Order Management
    # =========================================================================
    def send_order(self, req: OrderRequest) -> str:
        """주문 전송"""
        if not self.active:
            return ""
            
        asset_type = KisApiHelper.determine_asset_type(req.exchange, req.symbol)
        if not asset_type:
            self.write_log(f"지원하지 않는 자산 유형: {req.exchange}/{req.symbol}")
            return ""
        
        # 주문 ID 생성
        local_id = self._generate_order_id()
        req.orderid = local_id
        
        # 파라미터 빌드
        params = KisApiHelper.build_order_params(req, asset_type, self.main_account_no)
        is_real = (self.server_type == "REAL")
        action = "buy" if req.direction == Direction.LONG else "sell"
        tr_id = KisApiHelper.get_tr_id(asset_type, action, is_real, req.exchange)
        url = KisApiHelper.get_url_path(asset_type, action)
        
        # 주문 데이터 생성 및 저장
        order = req.create_order_data(local_id, self.gateway_name)
        order.status = Status.SUBMITTING
        self.order_map[local_id] = order
        self.on_order(order)
        
        # REST 요청 전송
        session = self.shared.get_session(is_real)
        if session:
            session.add_request(
                method="POST",
                path=url,
                data=params,
                extra={"tr_id": tr_id, "req": req, "local_id": local_id},
                callback=self._on_send_order_return
            )
            
        return f"{self.gateway_name}.{local_id}"

    def _on_send_order_return(self, data: dict, request: Request):
        """주문 응답 처리"""
        try:
            local_id = request.extra.get("local_id", "")
            req = request.extra.get("req")
            success, msg = KisApiHelper.check_response(data)
            
            if success:
                output = data.get("output", {})
                kis_odno = (
                    output.get("ODNO") or 
                    output.get("odno") or 
                    output.get("KRX_FWDG_ORD_ORGNO", "")
                )
                
                if kis_odno:
                    self.local_oid_map[local_id] = kis_odno
                    self.kis_oid_map[kis_odno] = local_id
                    self.write_log(f"주문 접수: {req.symbol} (Local:{local_id} <-> KIS:{kis_odno})")
                    
                    # 주문 상태 업데이트
                    order = self.order_map.get(local_id)
                    if order:
                        order.status = Status.NOTTRADED
                        self.on_order(order)
            else:
                self.write_log(f"주문 거부: {msg}")
                order = self.order_map.get(local_id)
                if order:
                    order.status = Status.REJECTED
                    self.on_order(order)
                    
        except Exception as e:
            logger.error(f"Order return handling error: {e}")

    def cancel_order(self, req: CancelRequest):
        """주문 취소"""
        org_odno = self.local_oid_map.get(req.orderid, req.orderid)
        asset_type = KisApiHelper.determine_asset_type(req.exchange, req.symbol)
        
        params = KisApiHelper.build_cancel_params(req, org_odno, asset_type, self.main_account_no)
        is_real = (self.server_type == "REAL")
        tr_id = KisApiHelper.get_tr_id(asset_type, "cancel", is_real, req.exchange)
        url = KisApiHelper.get_url_path(asset_type, "cancel")
        
        session = self.shared.get_session(is_real)
        if session:
            session.add_request(
                method="POST",
                path=url,
                data=params,
                extra={"tr_id": tr_id, "req": req},
                callback=self._on_cancel_order_return
            )

    def _on_cancel_order_return(self, data: dict, request: Request):
        """취소 응답 처리"""
        success, msg = KisApiHelper.check_response(data)
        if not success:
            self.write_log(f"주문 취소 거부: {msg}")
        else:
            req = request.extra.get("req")
            if req:
                self.write_log(f"주문 취소 접수: {req.symbol}")

    # =========================================================================
    # Quote Management (Two-sided Orders)
    # =========================================================================
    def send_quote(self, req: QuoteRequest) -> str:
        """양방향 호가 주문 전송 (매수+매도)"""
        self.quote_count += 1
        quote_id = str(self.quote_count)
        
        # 매수 주문
        buy_req = OrderRequest(
            symbol=req.symbol,
            exchange=req.exchange,
            direction=Direction.LONG,
            type=OrderType.LIMIT,
            price=req.bid_price,
            volume=req.bid_volume,
            offset=Offset.OPEN
        )
        vt_buy = self.send_order(buy_req)
        
        # 매도 주문
        sell_req = OrderRequest(
            symbol=req.symbol,
            exchange=req.exchange,
            direction=Direction.SHORT,
            type=OrderType.LIMIT,
            price=req.ask_price,
            volume=req.ask_volume,
            offset=Offset.CLOSE
        )
        vt_sell = self.send_order(sell_req)
        
        # Quote ID 매핑
        buy_id = vt_buy.split(".")[-1] if vt_buy else ""
        sell_id = vt_sell.split(".")[-1] if vt_sell else ""
        self.quote_map[quote_id] = (buy_id, sell_id)
        
        # Quote 이벤트 전송
        quote = QuoteData(
            gateway_name=self.gateway_name,
            symbol=req.symbol,
            exchange=req.exchange,
            quoteid=quote_id,
            bid_price=req.bid_price,
            bid_volume=req.bid_volume,
            ask_price=req.ask_price,
            ask_volume=req.ask_volume,
            bid_offset=Offset.OPEN,
            ask_offset=Offset.CLOSE,
            status=Status.NOTTRADED,
            datetime=datetime.now(KIS_TZ)
        )
        self.on_quote(quote)
        
        return f"{self.gateway_name}.{quote_id}"

    def cancel_quote(self, req: CancelRequest):
        """호가 주문 취소 (자식 주문 일괄 취소)"""
        quote_id = req.orderid
        child_orders = self.quote_map.get(quote_id)
        
        if not child_orders:
            return
        
        buy_id, sell_id = child_orders
        for oid in [buy_id, sell_id]:
            if oid:
                c_req = copy(req)
                c_req.orderid = oid
                self.cancel_order(c_req)
        
        if quote_id in self.quote_map:
            del self.quote_map[quote_id]

    # =========================================================================
    # Queries
    # =========================================================================
    def query_account(self):
        """계좌 잔고 조회"""
        if not self.active or not self.main_account_no:
            return
            
        is_real = (self.server_type == "REAL")
        session = self.shared.get_session(is_real)
        if not session:
            return
        
        # 자산 유형별 조회
        targets = [AssetType.KR_STOCK, AssetType.KR_FUTOPT, AssetType.OS_STOCK, AssetType.OS_FUTOPT]

        for at in targets:
            tr_id = KisApiHelper.get_tr_id(at, "balance", is_real)
            url = KisApiHelper.get_url_path(at, "balance")
            params = KisApiHelper.build_query_params(at, self.main_account_no, "balance")
            
            if url and tr_id:
                session.add_request(
                    method="GET",
                    path=url,
                    params=params,
                    data={},
                    extra={"tr_id": tr_id, "asset_type": at},
                    callback=self._on_balance_return
                )

    def _on_balance_return(self, data: dict, request: Request):
        """잔고 조회 응답 처리"""
        try:
            at = request.extra.get("asset_type")
            
            # 포지션 파싱
            positions = KisParser.parse_account_balance(self.gateway_name, data)
            for pos in positions:
                # Exchange 보정
                if pos.exchange == Exchange.KRX and at == AssetType.OS_STOCK:
                    pos.exchange = Exchange.NASDAQ
                self.on_position(pos)
            
            # 잔고 파싱
            balance = 0.0
            out = data.get("output2", []) or data.get("output1", [])
            if out and len(out) > 0:
                balance = float(out[0].get("dnca_tot_amt", 0) or out[0].get("frcr_evlu_tota", 0) or 0)
            
            account = AccountData(
                gateway_name=self.gateway_name,
                accountid=f"{self.main_account_no}_{at}",
                balance=balance,
                frozen=0
            )
            self.on_account(account)
            
        except Exception as e:
            logger.error(f"Balance return handling error: {e}")

    def query_position(self):
        """포지션 조회 (query_account로 대체)"""
        self.query_account()

    def query_history(self, req: HistoryRequest) -> List[Any]:
        """과거 데이터 조회"""
        if not self.datafeed:
            is_real = (self.server_type == "REAL")
            auth = self.shared.get_auth(is_real)
            if auth:
                self.datafeed = KisDatafeed(gateway=self)
                self.datafeed.init(app_key=auth.app_key, sec_key=auth.app_secret)
                
        if self.datafeed:
            return self.datafeed.query_bar_history(req)
        return []

    def query_contract(self, symbol: str, exchange: Exchange) -> Optional[ContractData]:
        """종목 정보 조회"""
        asset_type = KisApiHelper.determine_asset_type(exchange, symbol)
        is_real = (self.server_type == "REAL")
        tr_id, url, params = KisApiHelper.get_contract_search_params(asset_type, symbol, exchange, is_real)
        
        session = self.shared.get_session(is_real)
        if not url or not session:
            return None
            
        try:
            # 동기식 요청 (vnpy_rest에서 지원하는 경우)
            # 여기서는 비동기 패턴으로 처리
            # 실제 구현에서는 동기식 requests 사용 가능
            import requests as req_lib
            
            auth = self.shared.get_auth(is_real)
            base_url = auth.base_url
            full_url = f"{base_url}{url}"
            
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {auth.get_token()}",
                "appkey": auth.app_key,
                "appsecret": auth.app_secret,
                "tr_id": tr_id,
                "custtype": "P"
            }
            
            resp = req_lib.get(full_url, headers=headers, params=params, timeout=10)
            data = resp.json()
            
            contract = KisParser.parse_contract_info(self.gateway_name, data, asset_type)
            if contract:
                contract.symbol = symbol
                contract.exchange = exchange
                self.on_contract(contract)
                self.contract_map[symbol] = contract
                return contract
                
        except Exception as e:
            logger.error(f"Contract query error: {e}")
            
        return None

    # =========================================================================
    # Subscription & Interest List
    # =========================================================================
    def subscribe(self, req: SubscribeRequest):
        """실시간 시세 구독"""
        asset_type = KisApiHelper.determine_asset_type(req.exchange, req.symbol)
        if not asset_type:
            self.write_log(f"지원하지 않는 자산: {req.exchange}/{req.symbol}")
            return
        
        # 관심종목 저장
        self.subscribed[req.symbol] = req.exchange
        self._save_subscribed_list()

        # 종목 정보 조회 (없는 경우)
        if req.symbol not in self.contract_map:
            self.query_contract(req.symbol, req.exchange)

        # WebSocket 구독
        is_real = (self.server_type == "REAL")
        ws = self.shared.get_ws(is_real)
        
        if ws:
            # 국내 주식은 주식시간대에 따라 KRX 또는 SOR 사용
            target_exchange = req.exchange 
            if asset_type == AssetType.KR_STOCK and req.exchange in KR_STOCK_EXCHANGES:
                if market_state("KRX", datetime.now()) == "off":
                    target_exchange = Exchange.SOR
                else:
                    target_exchange = Exchange.KRX
                
            # TR ID 결정
            tick_tr = KisApiHelper.get_tr_id(asset_type, "tick", is_real, target_exchange)
            hoka_tr = KisApiHelper.get_tr_id(asset_type, "hoka", is_real, target_exchange)
            
            # 구독 키 결정
            key = req.symbol
            if asset_type == AssetType.OS_STOCK:
                ex_code = KisApiHelper.get_kis_exchange_code(asset_type, req.exchange, False)
                key = f"D{ex_code}{req.symbol}"
            
            # 구독 요청 (연결 전이면 pending queue에 추가됨)
            if tick_tr:
                ws.subscribe(self, tick_tr, key)
                # 로그에 명확히 기록
                logger.info(f"Subscribing tick: TR={tick_tr}, Key={key} ({req.symbol}[{req.exchange}->{target_exchange}])")
            if hoka_tr:
                ws.subscribe(self, hoka_tr, key)
                logger.info(f"Subscribing hoka: {hoka_tr}/{key}")

    def _load_subscribed_list(self):
        """구독 목록 로드 (String -> Enum 변환 필수)"""
        try:
            data = AtomicFileAdapter.load_json(Path(INTEREST_FILE))
            for sym, exchange_str in data.items():
                self.subscribed[sym] = Exchange(exchange_str)
        except Exception as e:
            self.write_log(f"구독 목록 로드 실패: {e}")


    def _save_subscribed_list(self):
        """관심종목 파일 저장"""
        try:
            data = { sym : exchange.value for sym, exchange in self.subscribed.items() }
            AtomicFileAdapter.save_json(Path(INTEREST_FILE), data)
        except Exception as e:
            self.write_log(f"구독 목록 저장 실패: {e}")

    def _auto_subscribe_interest(self):
        """관심종목 자동 구독"""
        if not self.subscribed:
            return
            
        self.write_log(f"관심종목 자동 구독 시작 ({len(self.subscribed)}개)")
        
        for sym, exchange in self.subscribed.items():
            try:
                req = SubscribeRequest(symbol=sym, exchange=exchange)
                self.subscribe(req)
            except Exception as e:
                logger.error(f"Auto subscribe error for {sym}: {e}")

    def _subscribe_notice_tr(self):
        """체결통보 구독"""
        if not self.user_id:
            return
            
        is_real = (self.server_type == "REAL")
        ws = self.shared.get_ws(is_real)
        
        if not ws:
            return
        
        # 체결통보 TR 목록
        if is_real:
            tr_list = ["H0STCNI0", "H0IFCNI0", "H0GSCNI0", "HDFFF1C0", "HDFFF2C0"]
        else:
            tr_list = ["H0STCNI9", "H0IFCNI9", "H0GSCNI0", "HDFFF1C0", "HDFFF2C0"]
        
        for tr in tr_list:
            ws.subscribe(self, tr, self.user_id)
        
        self.write_log(f"체결통보 구독 완료 (HTS ID: {self.user_id})")


    def unsubscribe(self, req: SubscribeRequest):
        """실시간 시세 구독 취소"""
        # 1. 자산 타입 및 TR ID 확인
        asset_type = KisApiHelper.determine_asset_type(req.exchange, req.symbol)
        if not asset_type:
            return

        # 국내 주식 심볼 보정 (구독 때와 동일한 로직 적용)
        clean_symbol = req.symbol
        target_exchange = req.exchange
        if asset_type == AssetType.KR_STOCK:
            target_exchange = Exchange.KRX
            if clean_symbol.startswith("A"):
                clean_symbol = clean_symbol[1:]
            if len(clean_symbol) < 6:
                clean_symbol = clean_symbol.zfill(6)

        # 2. WebSocket 연결 확인
        is_real = (self.server_type == "REAL")
        ws = self.shared.get_ws(is_real)
        
        if ws:
            # TR ID 조회
            tick_tr = KisApiHelper.get_tr_id(asset_type, "tick", is_real, target_exchange)
            hoka_tr = KisApiHelper.get_tr_id(asset_type, "hoka", is_real, target_exchange)
            
            # 키 생성
            key = clean_symbol
            if asset_type == AssetType.OS_STOCK:
                ex_code = KisApiHelper.get_kis_exchange_code(asset_type, req.exchange, False)
                key = f"D{ex_code}{clean_symbol}"

            # 구독 취소 요청 (WebSocket 클라이언트에 unsubscribe 메서드가 구현되어 있다고 가정)
            if tick_tr:
                ws.unsubscribe(self, tick_tr, key)
            if hoka_tr:
                ws.unsubscribe(self, hoka_tr, key)
                
            self.write_log(f"구독 취소 요청: {req.vt_symbol}")

    # =========================================================================
    # Internal Utils
    # =========================================================================
    def _load_account_config(self, alias: str) -> bool:
        """계좌 설정 로드"""
        if not os.path.exists(ACCOUNT_FILE):
            return False
            
        try:
            with open(ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                cfg = data.get(alias)
                
                if not cfg:
                    return False
                
                # 계좌번호 조합
                acc_no = cfg.get("account_no", "")
                acc_code = cfg.get("account_code", "01")
                self.main_account_no = f"{acc_no}{acc_code}"
                
                self.server_type = cfg.get("server", "REAL").upper()
                self.active_assets = cfg.get("assets", [])
                
                # HTS ID (파일에 저장된 경우)
                if "hts_id" in cfg and cfg["hts_id"]:
                    self.user_id = cfg["hts_id"]
                    
                return True
                
        except Exception as e:
            logger.error(f"Account config load error: {e}")
            return False

    def _extract_all_keys(self) -> dict:
        """모든 API 키 추출 (Shared Layer용)"""
        keys = {}
        
        if not os.path.exists(ACCOUNT_FILE):
            return keys
            
        try:
            with open(ACCOUNT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                for cfg in data.values():
                    srv = cfg.get("server", "REAL").upper()
                    ak = cfg.get("app_key", "")
                    sec = cfg.get("app_secret", "")
                    
                    if srv == "REAL" and "REAL_KEY" not in keys and ak:
                        keys["REAL_KEY"] = ak
                        keys["REAL_SECRET"] = sec
                    elif srv == "DEMO" and "DEMO_KEY" not in keys and ak:
                        keys["DEMO_KEY"] = ak
                        keys["DEMO_SECRET"] = sec
                        
        except Exception as e:
            logger.error(f"Key extraction error: {e}")
            
        return keys

    def _fix_exchange_info(self, data: Any):
        """Exchange 정보 보정"""
        if hasattr(data, 'exchange'):
            if not data.exchange or data.exchange == Exchange.LOCAL:
                c = self.contract_map.get(data.symbol)
                if c:
                    data.exchange = c.exchange

    def _load_master_data(self):
        """마스터 데이터 로드 (백그라운드)"""
        try:
            self.write_log("마스터 데이터 로딩 중...")
            
            # 국내 주식
            for mkt in ["kospi", "kosdaq", "konex"]:
                df = self.master_manager.get_data(mkt)
                self._reg_master(df, Exchange.KRX, Product.EQUITY, mkt.upper())
            
            # 해외 주식 (NASDAQ)
            df = self.master_manager.get_nasdaq()
            self._reg_master(df, Exchange.NASDAQ, Product.EQUITY, "NASDAQ")
            
            self.write_log(f"마스터 데이터 로드 완료 ({len(self.contract_map)}개 종목)")
            
        except Exception as e:
            self.write_log(f"마스터 데이터 로드 오류: {e}")
            logger.error(f"Master data load error: {e}")

    def _reg_master(self, df, exchange: Exchange, product: Product, market_type: str = "STOCK"):
        """마스터 데이터프레임을 순회하며 ContractData 생성"""
        if df is None or df.empty:
            return
        
        for _, row in df.iterrows():
            try:
                # Symbol & Name 매핑
                sym = ""
                name = ""
                
                if market_type in ["KOSPI", "KOSDAQ", "KONEX"]:
                    sym = str(row.get('단축코드') or row.get('Symbol') or "").strip()
                    name = str(row.get('한글명') or row.get('한글종목명') or "").strip()
                else:
                    sym = str(row.get('Symbol') or row.get('심볼') or "").strip()
                    name = str(row.get('Korea name') or row.get('한글명') or "").strip()
                    if not name:
                        name = str(row.get('English name') or row.get('영문명') or "").strip()

                if not sym:
                    continue

                # Price & Tick 매핑
                ref_price = 0.0
                min_vol = 1.0
                
                if market_type in ["KOSPI", "KOSDAQ"]:
                    val_price = row.get('기준가') or row.get('주식 기준가')
                    if val_price:
                        try:
                            ref_price = float(str(val_price).replace(',', ''))
                        except:
                            pass
                    
                    val_vol = row.get('매매수량단위') or row.get('정규 시장 매매 수량 단위')
                    if val_vol:
                        try:
                            min_vol = float(str(val_vol).replace(',', ''))
                        except:
                            pass
                            
                elif market_type == "NASDAQ":
                    val_price = row.get('base price')
                    if val_price:
                        try:
                            ref_price = float(val_price)
                        except:
                            pass
                    
                    val_vol = row.get('Bid order size')
                    if val_vol:
                        try:
                            min_vol = float(val_vol)
                        except:
                            pass

                # Pricetick 계산
                pricetick = 0.0
                if market_type in ["KOSPI", "KOSDAQ"]:
                    pricetick = get_krx_pricetick(ref_price, market_type)
                else:
                    pricetick = 0.01

                # ContractData 생성
                c = ContractData(
                    gateway_name=self.gateway_name,
                    symbol=sym,
                    exchange=exchange,
                    name=name,
                    product=product,
                    size=1,
                    pricetick=pricetick,
                    min_volume=min_vol,
                    history_data=True
                )
                self.contract_map[sym] = c
                self.on_contract(c)
                
            except Exception as e:
                continue
