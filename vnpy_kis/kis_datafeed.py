"""
KIS Datafeed — 통합 과거 봉 데이터 조회 (분/시/일/주/월)

- query_bar_history: KisApiHelper.determine_asset_type + build_history_params 기반
- MCP 기본시세: inquire_daily_itemchartprice, inquire_time_itemchartprice (국내주식/선옵/해외주식/해외선물)
- 1시간봉: 1분봉 조회 후 합성 (High/Low/Close/Volume)
- Pagination: tr_cont M/F 시 연속 조회 (CTX_AREA_FK/NK)

MCP Reference: search_domestic_stock_api 등 subcategory=\"기본시세\", function_name=\"inquire_daily_itemchartprice\" | \"inquire_time_*\"
"""

import requests
import time
import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from collections import defaultdict

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval, Product
from vnpy.trader.utility import get_folder_path

try:
    from .kis_api_helper import AssetType, KisApiHelper, KisConfig
    from .kis_shared import KisAuthManager
    from .kis_parser import KisParser, KIS_TZ
except ImportError:
    from vnpy_kis.kis_api_helper import AssetType, KisApiHelper, KisConfig
    from vnpy_kis.kis_shared import KisAuthManager
    from vnpy_kis.kis_parser import KisParser, KIS_TZ


class KisDatafeed(BaseDatafeed):
    """KIS 통합 데이터피드 (분/시/일/주/월봉, Pagination, 1시간봉 합성)."""
    def __init__(self, auth_manager=None, datafeed_name="KIS", gateway=None):
        self.auth_manager = auth_manager
        self.datafeed_name = datafeed_name
        self.active = False
        self.app_key = ""
        self.sec_key = ""
        self.gateway = gateway
        if not self.auth_manager:
            self._load_setting()
            
    def _load_setting(self):
        try:
            path = get_folder_path("kis")
            file_path = os.path.join(path, "kis_key.json")
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.app_key = data.get("app_key", "")
                    self.sec_key = data.get("app_secret", "")
        except Exception: pass

    def init(self, app_key: str = "", sec_key: str = "", gateway=None):
        """Datafeed 초기화 및 인증"""
        if self.auth_manager:
            self.active = True
            return True
        else:
            if app_key and sec_key:
                self.app_key = app_key
                self.sec_key = sec_key
            if not self.app_key or not self.sec_key:
                print("KIS Datafeed: No credentials found.")
                return False
            try:
                # Datafeed는 실전(REAL) 서버 데이터를 권장 (모의는 과거 데이터 제한적)
                self.auth_manager = KisAuthManager(self.app_key, self.sec_key, is_real=True)
                if self.auth_manager.get_token():
                    self.active = True
                    return True
            except Exception as e:
                print(f"KIS Datafeed Init Failed: {e}")
        return False

    def query_bar_history(self, req: HistoryRequest, output: Any = None) -> List[BarData]:
        """
        통합 과거 데이터 조회 진입점
        """
        if not self.active:
            self.init()
        
        # 1. 자산 타입 판별
        asset_type = KisApiHelper.determine_asset_type(req.exchange, req.symbol)
        if not asset_type:
            print(f"Unsupported Exchange/Symbol: {req.exchange} {req.symbol}")
            return []

        # 2. 시봉(1시간) 요청 시 최적화 분기
        if req.interval == Interval.HOUR:
            # [Case A] 해외 자산 (주식/선물) -> API가 60분봉 직접 지원 -> 바로 요청 (매우 빠름)
            if asset_type in [AssetType.OS_STOCK, AssetType.OS_FUTOPT]:
                print(f"🚀 [Speed-Up] {req.symbol} : 60분봉 직접 요청")
                # Interval.MINUTE 로직을 태우되, interval_num을 60으로 설정
                bars = self._query_history_loop(req, asset_type, interval_num=60)
                
                # 결과 Bar의 interval 속성을 HOUR로 보정
                for bar in bars:
                    bar.interval = Interval.HOUR
                return bars

            # [Case B] 국내 자산 -> API가 1분봉만 지원 -> 1분봉 수집 후 합성
            else:
                print(f"🔨 [Synthesis] {req.symbol} : 1분봉 수집 후 1시간봉 합성")
                return self._query_hourly_bars(req, asset_type)

        # 3. 일반 조회 (분봉, 일봉, 주봉 등)
        return self._query_history_loop(req, asset_type, interval_num=1)

    def _query_history_loop(self, req: HistoryRequest, asset_type: str, interval_num: int = 1) -> List[BarData]:
        """
        Pagination을 포함한 데이터 조회 루프
        """
        all_bars: List[BarData] = []
        next_ctx = {}
        max_loop = 100  # 무한 루프 방지
        loop_count = 0

        # Action 결정 (일봉 vs 분봉)
        is_daily_chart = req.interval in [Interval.DAILY, Interval.WEEKLY, Interval.MONTHLY]
        action = "daily" if is_daily_chart else "min"
        
        tr_id = KisApiHelper.get_tr_id(asset_type, action, is_real=True, exchange=req.exchange)
        url = KisApiHelper.get_url_path(asset_type, action)

        if not tr_id or not url:
            print(f"TR ID or URL not found for {asset_type} / {action}")
            return []

        while loop_count < max_loop:
            # 파라미터 빌드 (interval_num 전달)
            params = KisApiHelper.build_history_params(req, asset_type, next_ctx, interval_num=interval_num)
            
            # API 요청
            resp = self._send_request(url, tr_id, params)
            if not resp: 
                break
            
            data = resp.json()
            success, msg = KisApiHelper.check_response(data)
            if not success:
                # 데이터 없음(MCA00018)이나 권한 없음 등은 로그 남기고 종료
                if data.get("msg_cd") not in ["MCA00018", "EGW00123"]:
                    print(f"Query Failed ({req.symbol}): {msg}")
                break

            # 데이터 파싱
            bars = KisParser.parse_history_bar(
                self.gateway.gateway_name if self.gateway else "KIS", 
                data, req.symbol, req.exchange, req.interval
            )
            
            if not bars:
                break
            
            all_bars.extend(bars)

            # --- Pagination (연속 조회) 처리 ---
            # Header의 tr_cont 값 확인 (M/F: 연속데이터 있음, D/E: 없음)
            tr_cont = resp.headers.get("tr_cont", "D")
            if tr_cont not in ["M", "F"]:
                break
                
            body = data.get("output", {}) if "output" in data else data
            
            # 자산별 Next Key 추출 방식 상이
            if asset_type == AssetType.OS_STOCK and not is_daily_chart:
                # 해외주식 분봉: output block 내에 next/keyb 존재하지 않고, root level 혹은 output2에 있을 수 있음.
                # 보통 해외주식 분봉은 Body Root에 next, keyb가 있음 (첨부파일 분석 기반)
                next_ctx = {
                    "NEXT": data.get("next", ""),
                    "KEYB": data.get("keyb", "")
                }
                # 만약 키가 없으면 종료
                if not next_ctx["NEXT"] and not next_ctx["KEYB"]:
                    break
            else:
                # 국내주식 등 일반적인 경우 (CTX_AREA_FK 사용)
                next_ctx = {
                    "CTX_AREA_FK": body.get("ctx_area_fk", "") or body.get("ctx_area_fk100", "") or data.get("ctx_area_fk", ""),
                    "CTX_AREA_NK": body.get("ctx_area_nk", "") or body.get("ctx_area_nk100", "") or data.get("ctx_area_nk", "")
                }
                if not next_ctx.get("CTX_AREA_FK"):
                    break
            
            loop_count += 1
            time.sleep(0.2) # API 호출 제한 고려

        # 중복 제거 및 정렬
        unique_bars = {b.datetime: b for b in all_bars}
        sorted_bars = sorted(unique_bars.values(), key=lambda x: x.datetime)
        
        # 요청 기간 필터링
        # (KIS API는 요청한 날짜 이전 데이터도 뭉텅이로 주는 경우가 있어 필터링 필수)
        if sorted_bars:
            # timezone 정보가 있는 경우와 없는 경우를 맞춰줌
            req_start = req.start.replace(tzinfo=sorted_bars[0].datetime.tzinfo)
            req_end = req.end.replace(tzinfo=sorted_bars[0].datetime.tzinfo)
            result = [b for b in sorted_bars if req_start <= b.datetime <= req_end]
            return result
            
        return sorted_bars

    def _query_hourly_bars(self, req: HistoryRequest, asset_type: str) -> List[BarData]:
        """
        [국내 자산용] 1분봉을 받아 1시간봉으로 합성
        """
        # 1. 1분봉 데이터 요청
        req_min = HistoryRequest(
            symbol=req.symbol, 
            exchange=req.exchange, 
            start=req.start, 
            end=req.end, 
            interval=Interval.MINUTE
        )
        # 1분봉은 interval_num=1
        min_bars = self._query_history_loop(req_min, asset_type, interval_num=1)
        
        if not min_bars: 
            return []
            
        # 2. 합성 로직 (Resampling)
        hour_bars = []
        current_bar: Optional[BarData] = None
        
        for b in min_bars:
            # 09:15 -> 09:00 (시작 시간 기준 정렬)
            h_dt = b.datetime.replace(minute=0, second=0, microsecond=0)
            
            if current_bar is None:
                current_bar = BarData(
                    symbol=b.symbol, exchange=b.exchange, datetime=h_dt,
                    interval=Interval.HOUR, gateway_name=b.gateway_name,
                    open_price=b.open_price, high_price=b.high_price,
                    low_price=b.low_price, close_price=b.close_price,
                    volume=b.volume, turnover=b.turnover, open_interest=b.open_interest
                )
            elif current_bar.datetime == h_dt:
                # 기존 Bar 업데이트
                current_bar.high_price = max(current_bar.high_price, b.high_price)
                current_bar.low_price = min(current_bar.low_price, b.low_price)
                current_bar.close_price = b.close_price
                current_bar.volume += b.volume
                current_bar.turnover += b.turnover
                current_bar.open_interest = b.open_interest # OI는 보통 마지막 값 사용
            else:
                # 새로운 시간대 진입 -> 기존 Bar 저장 후 새로 생성
                hour_bars.append(current_bar)
                current_bar = BarData(
                    symbol=b.symbol, exchange=b.exchange, datetime=h_dt,
                    interval=Interval.HOUR, gateway_name=b.gateway_name,
                    open_price=b.open_price, high_price=b.high_price,
                    low_price=b.low_price, close_price=b.close_price,
                    volume=b.volume, turnover=b.turnover, open_interest=b.open_interest
                )
        
        # 마지막 Bar 추가
        if current_bar: 
            hour_bars.append(current_bar)
            
        return hour_bars

    def _send_request(self, url: str, tr_id: str, params: dict) -> Optional[requests.Response]:
        """REST 조회 (실전 서버 기준)."""
        base_url = KisAuthManager.get_base_url(is_real=True) 
        full_url = f"{base_url}{url}"
        
        token = self.auth_manager.get_token()
        if not token:
            return None

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.sec_key,
            "tr_id": tr_id,
            "custtype": "P"
        }
        
        try:
            return requests.get(full_url, headers=headers, params=params, timeout=10)
        except Exception as e:
            print(f"Request Error ({url}): {e}")
            return None