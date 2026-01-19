from datetime import datetime, time, timedelta
from typing import List, Dict, Optional, Callable
import pandas as pd
import numpy as np
from pykrx import stock
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.utility import ZoneInfo
from scipy.stats import norm

class KrxDatafeed(BaseDatafeed):
    """PyKRX 데이터를 통계 모델 기반으로 변환하는 데이터 피드"""
    
    def __init__(self):
        super().__init__()
        self.krx_timezone = ZoneInfo("Asia/Seoul")
        self.volatility_window = 20  # 변동성 계산 기간 (20일)
        
        # 지원하는 인터벌 매핑
        self.supported_intervals = {
            Interval.MINUTE: "1min",
            # Interval.MINUTE_5: "5min",
            # Interval.MINUTE_15: "15min",
            # Interval.MINUTE_30: "30min",
            Interval.HOUR: "1H",
            Interval.DAILY: "1D",
            Interval.WEEKLY: "1W",
        }

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """지정된 interval로 데이터 조회"""
        # 1. PyKRX에서 원본 데이터 가져오기
        base_df = self._get_pykrx_base_data(req)
        if base_df.empty:
            output(f"Historical data query failed")
            return [] 

        # 2. 요청된 interval로 데이터 변환
        converted_df = self._convert_timeframe(base_df, req.interval)
        if converted_df.empty:
            output("Data converting failed")
            return [] 

        # 3. VnPy BarData로 변환
        bars = self._convert_to_bardata(converted_df, req)
        if bars is None:
            output("Bar data is empty")
            return []    
            
        return bars
    

    def _get_pykrx_base_data(self, req: HistoryRequest) -> pd.DataFrame:
        """PyKRX에서 적절한 기간 데이터 조회"""
        start_str = req.start.strftime("%Y%m%d")
        end_str = req.end.strftime("%Y%m%d")

        try:
            if req.interval in [Interval.DAILY, Interval.MINUTE, Interval.MINUTE_5, 
                              Interval.MINUTE_15, Interval.MINUTE_30, Interval.HOUR]:
                df = stock.get_market_ohlcv_by_date(start_str, end_str, req.symbol)
            elif req.interval == Interval.WEEKLY:
                df = stock.get_market_ohlcv_by_date(start_str, end_str, req.symbol, freq="w")
            elif req.interval == Interval.MONTHLY:
                df = stock.get_market_ohlcv_by_date(start_str, end_str, req.symbol, freq="m")
            else:
                return pd.DataFrame()

            df = df.rename(columns={
                "시가": "open",
                "고가": "high",
                "저가": "low",
                "종가": "close",
                "거래량": "volume"
            })
            return df

        except Exception as e:
            print(f"PyKRX 데이터 조회 실패: {e}")
            return pd.DataFrame()

    def _convert_timeframe(self, df: pd.DataFrame, interval: Interval) -> pd.DataFrame:
        """데이터를 요청된 timeframe으로 변환"""
        if interval not in self.supported_intervals:
            print(f"지원하지 않는 인터벌: {interval}")
            return pd.DataFrame()

        # 일/주/월 데이터는 그대로 반환
        if interval in [Interval.DAILY, Interval.WEEKLY, Interval.MONTHLY]:
            return df

        # 분봉/시간봉 데이터 생성
        return self._generate_intraday_bars(df, interval)

    def _generate_intraday_bars(self, daily_df: pd.DataFrame, interval: Interval) -> pd.DataFrame:
        """통계 모델 기반 분봉 데이터 생성"""
        freq = self.supported_intervals[interval]
        all_minute_bars = []
        
        # 수익률 및 변동성 계산 (NaN 처리 강화)
        daily_df['returns'] = np.log(daily_df['close'] / daily_df['close'].shift(1))
        print('returns')
        
        # 변동성 계산 (최소 5일 데이터가 있을 때만 계산)
        min_periods = min(5, self.volatility_window)  # 최소 5일
        daily_df['volatility'] = daily_df['returns'].rolling(
            window=self.volatility_window,
            min_periods=min_periods
        ).std() * np.sqrt(252)
        
        # 변동성이 계산된 데이터만 사용
        valid_days = daily_df[daily_df['volatility'].notna()]
        if valid_days.empty:
            print("변동성 계산 실패: 충분한 데이터가 없습니다.")
            return pd.DataFrame()
        
        for date, daily_row in valid_days.iterrows():
            date = pd.to_datetime(date).tz_localize(self.krx_timezone)
            
            # 장 운영 시간 (09:00~15:30)
            timeline = pd.date_range(
                start=datetime.combine(date.date(), time(9, 0)),
                end=datetime.combine(date.date(), time(15, 30)),
                freq=freq,
                tz=self.krx_timezone
            )
            
            # GBM 모델 기반 가격 생성 (변동성 반영)
            prices = self._generate_gbm_prices(
                daily_row['open'],
                daily_row['volatility'],
                len(timeline),
                daily_row['high'],
                daily_row['low']
            )
            
            # 거래량 분배
            volumes = self._distribute_volume(
                daily_row['volume'],
                len(timeline),
                daily_row['volatility']
            )
            
            # 분봉 데이터 생성
            for i, (price, vol) in enumerate(zip(prices, volumes)):
                all_minute_bars.append({
                    'datetime': timeline[i],
                    'open': price[0],
                    'high': price[1],
                    'low': price[2],
                    'close': price[3],
                    'volume': vol
                })
        
        return pd.DataFrame(all_minute_bars).set_index('datetime') if all_minute_bars else pd.DataFrame()

    def _generate_gbm_prices(self, open_px, volatility, n_bars, high_bound, low_bound):
        """기하 브라운 운동 모델로 가격 생성"""
        prices = []
        current = open_px
        dt = 1/n_bars  # 시간 간격
        
        for _ in range(n_bars):
            # 무작위 충격 (정규 분포 기반)
            shock = norm.ppf(np.random.random(), scale=volatility*np.sqrt(dt))
            current *= np.exp(shock)
            
            # 일일 고저 범위 제한
            current = np.clip(current, low_bound, high_bound)
            
            # OHLC 생성 (시가=현재가, 고가/저가=변동 범위)
            o = current
            h = current * (1 + abs(shock)/2)
            l = current * (1 - abs(shock)/2)
            c = current * (1 + norm.ppf(np.random.random(), scale=volatility*np.sqrt(dt/10)))
            c = np.clip(c, l, h)  # 종가는 고가/저가 범위 내
            
            prices.append([o, h, l, c])
            current = c  # 다음 봉의 시가 = 현재 봉의 종가
        
        return prices

    def _distribute_volume(self, total_volume, n_bars, volatility):
        """변동성 고려한 현실적인 거래량 분배"""
        # 기본 U-모양 패턴 (장 시작/마감에 거래량 집중)
        base_weights = np.concatenate([
            np.linspace(2.0, 0.8, int(n_bars*0.3)),
            np.linspace(0.8, 0.8, int(n_bars*0.4)),
            np.linspace(0.8, 2.0, n_bars - int(n_bars*0.3) - int(n_bars*0.4))
        ])
        
        # 변동성에 따른 조정 (변동성 높을수록 거래량 분포 더 극적)
        adj_factor = 1 + (volatility / 0.3)  # 연간 변동성 30% 기준
        weights = base_weights * adj_factor
        weights = np.array(weights) / sum(weights) * total_volume
        
        return np.round(weights).astype(int)

    def _convert_to_bardata(self, df: pd.DataFrame, req: HistoryRequest) -> List[BarData]:
        """DataFrame을 VnPy BarData 리스트로 변환"""
        bars = []
        for dt, row in df.iterrows():
            bar = BarData(
                symbol=req.symbol,
                exchange=Exchange.KRX,
                datetime=dt.to_pydatetime(),
                interval=req.interval,
                open_price=row['open'],
                high_price=row['high'],
                low_price=row['low'],
                close_price=row['close'],
                volume=row['volume'],
                open_interest=0,
                gateway_name="KRX"
            )
            bars.append(bar)
        return bars