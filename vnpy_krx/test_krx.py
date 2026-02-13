from datetime import datetime, timedelta
import pandas as pd
from pykrx import stock

def get_kospi200_tickers():
    # 1. '코스피 200'이라는 이름에 해당하는 정확한 티커 찾기
    # 보통 1028 또는 101 입니다.
    index_list = stock.get_index_ticker_list(market="KOSPI")
    target_ticker = ""
    
    for s in index_list:
        name = stock.get_index_ticker_name(s)
        if name == "코스피 200":
            target_ticker = s
            break
    
    if not target_ticker:
        target_ticker = "1028"  # 기본값으로 1028 시도
    
    print(f"분석된 KOSPI 200 티커: {target_ticker}")

    # 2. 최근 10일간 역순으로 탐색하며 데이터가 있는 날짜 찾기
    for i in range(0, 10):
        search_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            # 지수 구성 종목 가져오기
            tickers = stock.get_index_portfolio_deposit_file(target_ticker, date=search_date)
            
            # DataFrame인 경우와 리스트인 경우 모두 대응
            if isinstance(tickers, pd.DataFrame):
                if not tickers.empty:
                    print(f"{search_date} 기준 데이터를 찾았습니다.")
                    return [f"{t}.KOSPI" for t in tickers.iloc[:, 0].tolist()]
            elif isinstance(tickers, list) and len(tickers) > 0:
                print(f"{search_date} 기준 데이터를 찾았습니다.")
                return [f"{t}.KOSPI" for t in tickers]
                
        except Exception as e:
            continue
            
    return []

# 실행
component_symbols = get_kospi200_tickers()

if component_symbols:
    print(f"조회 성공! 종목 수: {len(component_symbols)}개")
    print(f"샘플: {component_symbols[:5]}")
else:
    print("데이터를 가져오는 데 실패했습니다. 티커(1028/101)나 네트워크 상태를 확인하세요.")