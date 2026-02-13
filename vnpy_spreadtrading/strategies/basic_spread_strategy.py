from datetime import datetime
import numpy as np
import joblib
import pandas as pd
import os

from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy_spreadtrading import (
    SpreadStrategyTemplate,
    SpreadAlgoTemplate,
    TickData,
    BarData
)

class BybitBtcBasisStrategyV4(SpreadStrategyTemplate):
    """
    vn.py 표준(Basic & Statistical) 구조를 모두 통합한 
    Bybit BTC Basis ML 필터 전략
    """
    author = "An Seong-woo"

    # [매매 파라미터]
    entry_std = 2.0
    prob_limit = 0.65
    nq_symbol = "NQ2603.CME"
    nq_vol_limit = 50.0
    collect_data = True
    payup = 5
    interval = 5
    
    # [시간 제어]
    start_time = "00:00:00"
    end_time = "23:59:00"

    # [실시간 변수]
    current_basis = 0.0
    velocity = 0.0
    correlation = 0.0
    prob = 0.0
    nq_volatility = 0.0
    is_paused = False
    spread_pos = 0.0
    
    # [알고리즘 관리]
    long_algoid = ""
    short_algoid = ""

    parameters = [
        "entry_std", "prob_limit", "nq_symbol", "nq_vol_limit",
        "collect_data", "payup", "interval", "start_time", "end_time"
    ]
    variables = [
        "current_basis", "velocity", "correlation", "prob",
        "nq_volatility", "is_paused", "spread_pos",
        "long_algoid", "short_algoid"
    ]

    def __init__(self, strategy_manager, strategy_name, spread, setting):
        super().__init__(strategy_manager, strategy_name, spread, setting)
        
        # ML 모델 로드
        try:
            self.model = joblib.load("btc_ml_filter.pkl")
        except:
            self.model = None
            self.write_log("ML 모델 로드 실패: 필터 없이 실행됩니다.")

        # 분석 도구 및 시간 설정
        self.bg = BarGenerator(self.on_spread_bar)
        self.am_basis = ArrayManager()
        self.am_nq = ArrayManager()
        
        self.start_t = datetime.strptime(self.start_time, "%H:%M:%S").time()
        self.end_t = datetime.strptime(self.end_time, "%H:%M:%S").time()
        
        self.raw_data_list = []

    def on_init(self):
        self.write_log("전략 초기화: 데이터 로딩 시작")
        self.load_bar(10)

    def on_start(self):
        self.write_log("전략 시작: Bybit BTC 베이시스 감시")

    def on_stop(self):
        self.write_log("전략 중지 및 잔여 데이터 저장")
        if self.raw_data_list:
            pd.DataFrame(self.raw_data_list).to_csv("ml_raw_data.csv", mode='a', index=False)
        
        # 알고리즘 ID 초기화
        self.long_algoid = ""
        self.short_algoid = ""
        self.put_event()

    def on_spread_data(self):
        """실시간 스프레드 데이터 수신 시 호출"""
        # 1. 거래 시간 확인
        update_time = self.spread.datetime.time()
        if update_time < self.start_t or update_time >= self.end_t:
            self.stop_all_algos()
            return

        tick = self.get_spread_tick()
        self.on_spread_tick(tick)

    def on_spread_tick(self, tick: TickData):
        """틱 기반 로직 (데이터 수집 및 BG 업데이트)"""
        self.current_basis = tick.bid_price_1 - tick.ask_price_1
        
        if self.collect_data:
            self.record_raw_data(tick)

        self.bg.update_tick(tick)

    def on_spread_bar(self, bar: BarData):
        """바 기반 로직 (신호 계산 및 알고리즘 관리)"""
        # 기존 진행 중인 알고리즘이 없다면 새로 계산
        self.am_basis.update_bar(bar)
        
        nq_tick = self.get_leg_tick(self.nq_symbol)
        if nq_tick:
            self.am_nq.update_tick(nq_tick)

        if not self.am_basis.inited or not self.am_nq.inited:
            return

        # [지표 계산]
        self.velocity = self.am_basis.close[-1] - self.am_basis.close[-5]
        self.nq_volatility = np.max(self.am_nq.close[-20:]) - np.min(self.am_nq.close[-20:])
        self.is_paused = self.nq_volatility > self.nq_vol_limit
        
        if len(self.am_basis.close) >= 30:
            self.correlation = np.corrcoef(self.am_basis.close[-30:], self.am_nq.close[-30:])[0, 1]

        if self.is_paused:
            self.stop_all_algos()
            return

        # [ML 예측]
        ma = self.am_basis.sma(20)
        std = self.am_basis.std(20)
        
        tick = self.get_spread_tick()
        f_imb = (tick.bid_volume_1 - tick.ask_volume_1) / (tick.bid_volume_1 + tick.ask_volume_1 + 1e-6)
        features = np.array([[self.current_basis, self.velocity, self.correlation, f_imb]])
        
        self.prob = self.model.predict_proba(features)[0][1] if self.model else 1.0

        # [매매 로직 - BasicSpreadStrategy 방식의 ID 관리 적용]
        self.spread_pos = self.get_spread_pos()

        # 1. 무포지션 시 진입 (Short Basis)
        if not self.spread_pos:
            if bar.close_price > ma + (std * self.entry_std):
                if self.prob > self.prob_limit and abs(self.velocity) < 10.0:
                    if not self.short_algoid: # 중복 방지
                        self.short_algoid = self.start_short_algo(
                            bar.close_price - 5, 0.01, self.payup, self.interval
                        )

        # 2. 숏 포지션 보유 중 청산
        elif self.spread_pos < 0:
            if bar.close_price <= ma:
                if not self.long_algoid: # 중복 방지
                    self.long_algoid = self.start_long_algo(
                        bar.close_price + 5, abs(self.spread_pos), self.payup, self.interval
                    )

        self.put_event()

    def on_spread_pos(self):
        """포지션 업데이트 콜백"""
        self.spread_pos = self.get_spread_pos()
        self.put_event()

    def on_spread_algo(self, algo: SpreadAlgoTemplate):
        """알고리즘 상태 업데이트 시 ID 초기화"""
        if not algo.is_active():
            if algo.algoid == self.long_algoid:
                self.long_algoid = ""
            elif algo.algoid == self.short_algoid:
                self.short_algoid = ""
        self.put_event()

    def record_raw_data(self, tick: TickData):
        """실시간 데이터 수집"""
        data = {
            "datetime": datetime.now(),
            "basis": self.current_basis,
            "velocity": self.velocity,
            "correlation": self.correlation,
            "nq_vol": self.nq_volatility,
            "f_bid_vol": tick.bid_volume_1,
            "f_ask_vol": tick.ask_volume_1,
            "pos": self.get_spread_pos()
        }
        self.raw_data_list.append(data)
        
        if len(self.raw_data_list) >= 500:
            pd.DataFrame(self.raw_data_list).to_csv("ml_raw_data.csv", mode='a', header=not os.path.exists("ml_raw_data.csv"), index=False)
            self.raw_data_list = []