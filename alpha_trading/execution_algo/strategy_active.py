# Layer 3: Execution Algorithm (Execution Strategy)
# The Trader

"""
Active / Chase Strategy for unfilled orders.
- Amend order price to chase the market.
- Force-fill orders before market close.
"""

def chase_unfilled_orders(unfilled_orders):
    """
    If a limit order is not filled after N minutes, amend the price
    to be more aggressive.
    """
    print("Chasing unfilled orders by amending price...")
    # 미체결 주문 조회
    # 지정가 정정 주문 전송 (호가 1~2단계 위로)
    pass

def execute_market_orders_before_close(unfilled_orders_with_high_alpha):
    """
    For high-alpha stocks, force-fill any remaining orders with
    a market order before the session closes.
    """
    print("Force-filling high-alpha orders with market orders before close.")
    # 장 마감 N분 전, Alpha 강도가 높은 미체결 종목에 대해 시장가 주문 전송
    pass
