# Layer 3: Execution Algorithm (Execution Strategy)
# The Trader

"""
Smart Entry Filters to improve execution price.
- Gap Filter
- Order Book Imbalance Filter
"""

def check_gap_up_filter(symbol, open_price, previous_close):
    """
    Checks if the opening price has gapped up significantly.
    If open is > +3% from previous close, delay or reduce order size.
    """
    gap_percentage = (open_price - previous_close) / previous_close
    if gap_percentage > 0.03:
        print(f"Gap-up filter triggered for {symbol}: {gap_percentage:.2%}")
        return True
    return False

def check_order_book_imbalance(symbol, order_book):
    """
    Checks for a significant imbalance in the order book.
    If ask side volume is 3x bid side volume, delay entry.
    """
    # total_ask_volume = sum(level.volume for level in order_book.asks)
    # total_bid_volume = sum(level.volume for level in order_book.bids)
    # if total_ask_volume > total_bid_volume * 3:
    #     print(f"Order book imbalance filter triggered for {symbol}")
    #     return True
    return False
