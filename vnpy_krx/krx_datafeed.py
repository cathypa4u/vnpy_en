from datetime import datetime, time, timedelta
from typing import List, Optional, Callable
import pandas as pd
import numpy as np
from pykrx import stock, bond
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.utility import ZoneInfo

class KrxDatafeed(BaseDatafeed):
    def __init__(self):
        super().__init__()
        self.krx_timezone = ZoneInfo("Asia/Seoul")
        
    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        try:
            df = self._fetch_from_pykrx(req)
            
            if df is None or df.empty:
                output(f"[{req.symbol}] 데이터를 찾을 수 없습니다.")
                return []

            if req.interval == Interval.DAILY:
                return self._convert_to_daily_bars(df, req)
            
            # 분/시봉 합성 로직 (채권의 경우 수익률 기반으로 합성됨)
            elif req.interval in [Interval.HOUR, Interval.MINUTE]:
                steps = 7 if req.interval == Interval.HOUR else 390
                return self._generate_synthetic_bars(df, req, steps_per_day=steps)
            
            return []
            
        except Exception as e:
            output(f"KrxDatafeed 오류: {e}")
            return []

    def _fetch_from_pykrx(self, req: HistoryRequest) -> pd.DataFrame:
        """자산별 최적 API 호출 (채권 수익률 포함)"""
        s = req.start.strftime("%Y%m%d")
        e = req.end.strftime("%Y%m%d")
        symbol = req.symbol
        
        # 1. 지수(Index) 판별
        if len(symbol) == 4 and symbol.isdigit():
            # 출력창에 어떤 API를 타는지 로그를 남기면 디버깅이 쉽습니다.
            print(f"Calling Index API for {symbol}")            
            return stock.get_index_ohlcv_by_date(s, e, symbol)
        
        # 2. 채권(Bond/Treasury) 판별
        # 심볼이 국고채 명칭(예: '국고채3년')이거나 채권 관련 요청일 경우
        if any(keyword in symbol for keyword in ["국고", "채권", "T-Bill", "Treasury"]):
            # 말씀하신 장외 일자별 수익률 함수 사용
            return bond.get_otc_treasury_yields(s, e, symbol)

        # 3. 주식/ETF 판별
        df = stock.get_market_ohlcv(s, e, symbol)
        if df.empty:
            df = stock.get_etf_ohlcv_by_date(s, e, symbol)
            
        return df

    def _convert_to_daily_bars(self, df: pd.DataFrame, req: HistoryRequest) -> List[BarData]:
        """자산별 컬럼 구조(가격 vs 수익률)에 따른 변환"""
        bars = []
        for dt, row in df.iterrows():
            bar_dt = pd.to_datetime(dt).to_pydatetime().replace(tzinfo=self.krx_timezone)
            
            # 기본값
            o, h, l, c, v = 0.0, 0.0, 0.0, 0.0, 0.0
            
            # A. 채권 수익률 데이터 처리 (get_otc_treasury_yields 대응)
            if '수익률' in df.columns:
                # 채권은 수익률을 가격 필드에 저장 (퀀트 분석용)
                val = float(row['수익률'])
                o = h = l = c = val
                # 채권 데이터에 '대비' 등이 있을 수 있으나 거래량은 없을 수 있음
                v = float(row.get('거래량', 0))
                
            # B. 일반 주식/지수/ETF (OHLCV 구조)
            elif '시가' in df.columns:
                o, h, l, c = float(row['시가']), float(row['고가']), float(row['저가']), float(row['종가'])
                v = float(row.get('거래량', 0))
            
            # C. 기타 예외 상황 (위치 기반 매핑)
            else:
                try:
                    c = float(row.iloc[0]) # 첫 번째 컬럼을 종가로 간주
                    o = h = l = c
                except: continue

            bar = BarData(
                symbol=req.symbol, exchange=req.exchange, datetime=bar_dt,
                interval=Interval.DAILY, open_price=o, high_price=h,
                low_price=l, close_price=c, volume=v, gateway_name="KRX"
            )
            bars.append(bar)
        return bars

    def _generate_synthetic_bars(self, df: pd.DataFrame, req: HistoryRequest, steps_per_day: int) -> List[BarData]:
        """Brownian Bridge 기반 하위 인터벌 합성 (가격/수익률 공통 적용)"""
        all_bars = []
        market_open_time = time(9, 0)
        
        for dt, row in df.iterrows():
            # 컬럼 구조 유연하게 대응
            if '시가' in row:
                o, h, l, c = float(row['시가']), float(row['고가']), float(row['저가']), float(row['종가'])
            else:
                # 채권 수익률 등 OHLC가 없는 경우
                val = float(row.get('수익률', row.iloc[0]))
                o = h = l = c = val
            
            v = float(row.get('거래량', 0))

            # 시뮬레이션 경로 생성
            noise = np.random.normal(0, 1, steps_per_day)
            path = np.cumsum(noise)
            bridge = path - np.linspace(0, path[-1], steps_per_day)
            
            # 변동성 부여 (수익률 데이터면 아주 작은 변동성만 부여)
            vol = (h - l) if (h - l) > 0 else (c * 0.0001)
            synth_prices = bridge * (vol / (bridge.max() - bridge.min())) if (bridge.max() - bridge.min()) > 0 else np.full(steps_per_day, o)
            synth_prices += (o - synth_prices[0])
            synth_prices[-1] = c # 종가 고정

            base_dt = pd.to_datetime(dt).to_pydatetime().replace(hour=9, tzinfo=self.krx_timezone)
            
            for i in range(steps_per_day):
                bar_dt = base_dt + (timedelta(hours=i) if req.interval == Interval.HOUR else timedelta(minutes=i))
                p = float(synth_prices[i])
                all_bars.append(BarData(
                    symbol=req.symbol, exchange=req.exchange, datetime=bar_dt,
                    interval=req.interval, open_price=p, high_price=p, low_price=p, close_price=p,
                    volume=int(v/steps_per_day) if v > 0 else 0, gateway_name="KRX"
                ))
        return all_bars