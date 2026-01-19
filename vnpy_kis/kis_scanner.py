"""
KIS Market Scanner for Vn.py
Desc: Fetches fundamentals and calculates technical indicators for screening/rebalancing.
"""

import requests
import pandas as pd
import numpy as np
import time
from typing import Dict, Any, List
from vnpy.trader.setting import SETTINGS
from .kis_api import kis_auth  # <--- 중앙 인증 모듈 Import

# --- 설정 및 상수 ---
REST_HOST_REAL = "https://openapi.koreainvestment.com:9443"
REST_HOST_DEMO = "https://openapivts.koreainvestment.com:29443"

class KisScanner:
    def __init__(self):
        # 1. 설정 로드
        self.app_key = SETTINGS.get("kis.app_key", "")
        self.app_secret = SETTINGS.get("kis.app_secret", "")
        self.server = SETTINGS.get("kis.server", "REAL")
        
        # self.domain = REST_HOST_DEMO if self.server == "DEMO" else REST_HOST_REAL
        # self.access_token = None

        # 2. Auth Manager 설정 (이미 Gateway에서 설정했다면 생략되지만, 단독 실행 대비)
        if self.app_key and self.app_secret:
            kis_auth.configure(self.app_key, self.app_secret, self.server)
        
        # 도메인은 Auth Manager에 설정된 값을 참조
        self.domain = kis_auth.domain
        
        # API Rate Limit 관리를 위한 딜레이 (초)
        self.request_delay = 0.1 

    def _get_header(self, tr_id):
        """
        kis_auth를 통해 유효한 토큰을 가져와 헤더 생성
        """
        token = kis_auth.get_token() # <--- 중앙 관리 토큰 호출
        
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": kis_auth.app_key,       # Auth Manager의 Key 사용
            "appsecret": kis_auth.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def get_market_data(self, code: str) -> Dict[str, Any]:
        """
        [펀더멘털 & 현재가 상태 조회]
        TR: FHKST01010100 (주식현재가 시세)
        """
        url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-price"
        
        # 토큰 만료 시 kis_auth가 알아서 갱신하므로 try-except 불필요 (Auth 내부 처리)        
        headers = self._get_header("FHKST01010100")
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code
        }
        
        try:
            res = requests.get(url, headers=headers, params=params)
            data = res.json().get('output', {})
            
            # 필요한 데이터 추출 및 형변환
            result = {
                "symbol": code,
                "name": data.get("rprs_mrkt_kor_name", ""), # 한글명
                "price": float(data.get("stck_prpr", 0)),   # 현재가
                "market_cap": float(data.get("hts_avls", 0)), # 시가총액 (억)
                "per": float(data.get("per", 0)),
                "pbr": float(data.get("pbr", 0)),
                "eps": float(data.get("eps", 0)),
                "high_250d": float(data.get("d250_hgpr", 0)), # 250일 최고
                "low_250d": float(data.get("d250_lwpr", 0)),  # 250일 최저
                "vol_rotation": float(data.get("vol_tnrt", 0)) # 거래량 회전율
            }
            time.sleep(self.request_delay) # Rate Limit 방지
            return result
        except Exception as e:
            print(f"Error fetching fundamental for {code}: {e}")
            return {}

    def get_technical_indicators(self, code: str, period: int = 60) -> Dict[str, Any]:
        """
        [기술적 지표 계산]
        TR: FHKST03010100 (일봉 조회) -> Pandas -> Indicator
        """
        url = f"{self.domain}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = self._get_header("FHKST03010100")
        
        # 오늘 날짜 기준 최근 period 일수 조회
        import datetime
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=period*2)).strftime("%Y%m%d")
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "1"
        }
        
        try:
            res = requests.get(url, headers=headers, params=params)
            items = res.json().get('output2', [])
            
            if not items:
                return {}

            # DataFrame 생성
            df = pd.DataFrame(items)
            df = df[['stck_bsop_date', 'stck_clpr', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'acml_vol']]
            df.columns = ['Date', 'Close', 'Open', 'High', 'Low', 'Volume']
            df = df.astype({'Close': float, 'Open': float, 'High': float, 'Low': float, 'Volume': float})
            df = df.sort_values('Date').reset_index(drop=True) # 날짜 오름차순 정렬

            # --- 지표 계산 (Vectorized Calculation) ---
            
            # 1. 이동평균선 (MA)
            df['MA20'] = df['Close'].rolling(window=20).mean()
            df['MA60'] = df['Close'].rolling(window=60).mean()
            
            # 2. RSI (14일)
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # 3. 이격도 (Disparity)
            df['Disparity20'] = (df['Close'] / df['MA20']) * 100
            
            # 최근 값 추출
            if len(df) > 0:
                latest = df.iloc[-1]
                
                result = {
                    "ma20": latest['MA20'],
                    "ma60": latest['MA60'],
                    "rsi": latest['RSI'],
                    "disparity_20": latest['Disparity20'],
                    "trend": "UP" if latest['Close'] > latest['MA20'] else "DOWN"
                }
            else:
                return {}
                        
            time.sleep(self.request_delay)
            return result
            
        except Exception as e:
            print(f"Error calculating technicals for {code}: {e}")
            return {}

    def analyze_portfolio(self, codes: List[str]):
        """
        종목 리스트를 받아 종합 데이터를 리턴 (리밸런싱용)
        """
        results = []
        print(f"Scanning {len(codes)} stocks...")
        
        for code in codes:
            fund = self.get_market_data(code)
            tech = self.get_technical_indicators(code)
            
            # 두 데이터 병합
            combined = {**fund, **tech}
            if combined:
                results.append(combined)
                
        return pd.DataFrame(results)

# --- 사용 예시 ---
if __name__ == "__main__":
    scanner = KisScanner()
    
    # 관심 종목 리스트 (예: 삼성전자, 하이닉스, 현대차)
    my_portfolio = ["005930", "000660", "005380"]
    
    df = scanner.analyze_portfolio(my_portfolio)
    
    # 예시: PER 10 이하이고 RSI가 30 이하인 저평가 과매도 종목 찾기
    filtered = df[ (df['per'] < 10) & (df['rsi'] < 40) ]
    
    print("=== 분석 결과 ===")
    print(df[['name', 'price', 'per', 'pbr', 'rsi', 'trend']])
    
    print("\n=== 추천 종목 (저평가 & 과매도) ===")
    print(filtered)