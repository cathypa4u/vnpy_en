# Layer 3: Execution Algorithm (Execution Strategy)
# The Trader

"""
Main trading loop.
- Connects to vnpy engine.
- Manages order lifecycle based on signals from Layer 2.
"""

class AlgoTrader:
    def __init__(self, vnpy_engine):
        self.engine = vnpy_engine
        print("AlgoTrader initialized, connected to vnpy.")

    def run(self):
        """The main trading loop that runs during market hours."""
        print("Starting main trading loop...")
        self.check_filters()
        self.execute_passive_strategy()
        self.handle_unfilled_orders()
        print("Trading loop finished.")

    def check_filters(self):
        """Apply entry filters before placing orders."""
        print("Applying pre-trade filters (Gap, Order Book Imbalance)...")
        # filters.py의 함수들 호출

    def execute_passive_strategy(self):
        """Execute the passive (Sniper) strategy."""
        print("Executing passive 'Sniper' strategy...")
        # strategy_passive.py 로직 실행

    def handle_unfilled_orders(self):
        """Handle unfilled orders using the Chase logic."""
        print("Executing 'Chase' logic for unfilled orders...")
        # strategy_active.py 로직 실행
