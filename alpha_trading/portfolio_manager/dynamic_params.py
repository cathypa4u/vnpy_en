# Layer 2: Portfolio Manager (Portfolio Strategy)
# The Fund Manager

"""
Walk-Forward Optimization for dynamic parameters.
- Dynamic Top-K selection
- Dynamic Stop-Loss adjustment
"""

def optimize_top_k(backtest_results):
    """
    Determine the optimal number of positions (K) for the next period
    based on recent (e.g., 3-month) simulation results.
    """
    print("Optimizing Top-K parameter...")
    # 직전 3개월 시뮬레이션 결과 분석
    # Top-5 vs Top-20 등 성과 비교하여 K 결정
    return 10 # Example K

def optimize_stop_loss(market_volatility):
    """
    Adjust the stop-loss percentage based on current market volatility.
    Wider in volatile markets, tighter in stable markets.
    """
    print("Optimizing Stop-Loss parameter...")
    # 시장 변동성 지표(e.g., V-KOSPI) 분석
    # 변동성에 따라 손절폭 자동 조절
    return 0.05 # Example 5% stop-loss
