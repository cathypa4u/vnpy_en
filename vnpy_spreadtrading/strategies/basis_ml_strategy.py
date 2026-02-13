from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy_spreadtrading import (
    SpreadStrategyTemplate,
    SpreadAlgoTemplate,
    TickData,
    BarData
)
import numpy as np
import joblib
import pandas as pd
import os
from datetime import datetime


class BybitBtcBasisStrategy(SpreadStrategyTemplate):
    """
    표준 vnpy 인터페이스를 준수한 Bybit BTC Basis ML 필터 전략
    """
    author = "An Seong-woo"

    # 파라미터 선언 (UI 노출용)
    entry_std = 2.0
    prob_limit = 0.65
    nq_symbol = "NQ2603.CME"
    nq_vol_limit = 50.0
    collect_data = True
    payup = 2
    interval = 5

    # 변수 선언 (UI 실시간 모니터링용)
    current_basis = 0.0
    velocity = 0.0
    correlation = 0.0
    prob = 0.0
    nq_volatility = 0.0
    is_paused = False
    spread_pos = 0.0

    parameters = [
        "entry_std",
        "prob_limit",
        "nq_symbol",
        "nq_vol_limit",
        "collect_data",
        "payup",
        "interval"
    ]
    variables = [
        "current_basis",
        "velocity",
        "correlation",
        "prob",
        "nq_volatility",
        "is_paused",
        "spread_pos"
    ]

    def __init__(self, strategy_manager, strategy_name, spread, setting):
        super().__init__(strategy_manager, strategy_name, spread, setting)
        
        # ML 모델 로드
        try:
            self.model = joblib.load("btc_ml_filter.pkl")
        except:
            self.model = None
            self.write_log("ML 모델 로드 실패: 필터 없이 실행됩니다.")

        # 분석 도구 초기화
        self.bg = BarGenerator(self.on_spread_bar)
        self.am_basis = ArrayManager()
        self.am_nq = ArrayManager()
        
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
        self.put_event()

    def on_spread_data(self):
        """실시간 스프레드 데이터 수신 시 호출"""
        tick = self.get_spread_tick()
        self.on_spread_tick(tick)

    def on_spread_tick(self, tick: TickData):
        """틱 기반 로직 (데이터 수집 및 변동성 필터 계산)"""
        # 1. 기본 베이시스 계산 및 틱 업데이트
        self.current_basis = tick.bid_price_1 - tick.ask_price_1
        
        # 2. 데이터 수집 (Raw Data 저장)
        if self.collect_data:
            self.record_raw_data(tick)

        # 3. BarGenerator 업데이트 (분봉 생성용)
        self.bg.update_tick(tick)

    def on_spread_bar(self, bar: BarData):
        """바 기반 로직 (신호 계산 및 집행)"""
        self.stop_all_algos()

        # 1. 기초 데이터 업데이트
        self.am_basis.update_bar(bar)
        
        # NQ 데이터 참조
        nq_tick = self.get_leg_tick(self.nq_symbol)
        if nq_tick:
            # NQ 틱을 임시 바 데이터로 변환하여 ArrayManager 업데이트
            self.am_nq.update_tick(nq_tick)

        if not self.am_basis.inited or not self.am_nq.inited:
            return

        # 2. 피처 계산 (Velocity, Correlation, Volatility)
        self.velocity = self.am_basis.close[-1] - self.am_basis.close[-5]
        self.nq_volatility = np.max(self.am_nq.close[-20:]) - np.min(self.am_nq.close[-20:])
        self.is_paused = self.nq_volatility > self.nq_vol_limit
        
        if len(self.am_basis.close) >= 30:
            self.correlation = np.corrcoef(self.am_basis.close[-30:], self.am_nq.close[-30:])[0, 1]

        # 3. 진입 필터 및 ML 예측
        if self.is_paused:
            return

        ma = self.am_basis.sma(20)
        std = self.am_basis.std(20)
        
        # ML 피처 생성 (틱 데이터 활용)
        tick = self.get_spread_tick()
        f_imb = (tick.bid_volume_1 - tick.ask_volume_1) / (tick.bid_volume_1 + tick.ask_volume_1 + 1e-6)
        features = np.array([[self.current_basis, self.velocity, self.correlation, f_imb]])
        
        self.prob = self.model.predict_proba(features)[0][1] if self.model else 1.0

        # 4. 진입 및 청산 로직
        self.spread_pos = self.get_spread_pos()

        # 진입: 볼린저 상단 돌파 + ML 고승률 + 속도 제어
        if self.spread_pos <= 0:
            if bar.close_price > ma + (std * self.entry_std):
                if self.prob > self.prob_limit and abs(self.velocity) < 10.0:
                    self.start_short_algo(bar.close_price - 5, 0.01, self.payup, self.interval)

        # 청산: 평균 회귀
        elif self.spread_pos < 0:
            if bar.close_price <= ma:
                self.start_long_algo(bar.close_price + 5, abs(self.spread_pos), self.payup, self.interval)

        self.put_event()

    def record_raw_data(self, tick: TickData):
        """실시간 피처 기록"""
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
            df = pd.DataFrame(self.raw_data_list)
            df.to_csv("ml_raw_data.csv", mode='a', header=not os.path.exists("ml_raw_data.csv"), index=False)
            self.raw_data_list = []

    def on_spread_pos(self):
        """포지션 업데이트 콜백"""
        self.spread_pos = self.get_spread_pos()
        self.put_event()

    def on_spread_algo(self, algo: SpreadAlgoTemplate):
        pass