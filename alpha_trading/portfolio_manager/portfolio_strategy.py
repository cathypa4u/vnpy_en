from datetime import datetime
import pandas as pd
import numpy as np
from typing import Dict, List

from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import ArrayManager
from vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine

from .allocation import calculate_inverse_volatility_weights, calculate_hrp_weights
from .optimization import optimize_top_k, optimize_stop_loss

class AlphaPortfolioStrategy(StrategyTemplate):
    """
    Layer 2: Portfolio Manager Strategy
    
    Features:
    - Dynamic Top-K Selection (Walk-Forward Optimization)
    - Dynamic Stop-Loss based on Volatility
    - Weighting: Inverse Volatility or HRP
    """
    author = "AlphaFactory"
    
    # Parameters
    lookback_window = 90        # Days for history (approx 3 months for WFO)
    rebalance_interval = 20     # Trading days (approx 1 month)
    base_stop_loss = 0.05
    use_hrp = True              # Use HRP (True) or Inverse Volatility (False)
    capital = 100_000_000       # Target capital for allocation
    
    # Variables
    current_k = 10
    current_stop_loss = 0.05
    last_rebalance_date = None
    
    parameters = [
        "lookback_window", 
        "rebalance_interval", 
        "base_stop_loss", 
        "use_hrp",
        "capital"
    ]
    variables = [
        "current_k", 
        "current_stop_loss", 
        "last_rebalance_date"
    ]

    def __init__(
        self,
        strategy_engine: StrategyEngine,
        strategy_name: str,
        vt_symbols: List[str],
        setting: dict
    ) -> None:
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)
        
        self.ams: Dict[str, ArrayManager] = {}
        self.signals: pd.DataFrame = pd.DataFrame()
        
        # Initialize ArrayManagers for history
        for vt_symbol in vt_symbols:
            self.ams[vt_symbol] = ArrayManager(size=self.lookback_window + 20)
            
    def on_init(self) -> None:
        self.write_log("Portfolio Strategy Initializing...")
        self.load_bars(self.lookback_window)
        
        # TODO: Load external signals here (e.g., from CSV or Database)
        # For now, we assume signals are injected or loaded via a helper
        # self.signals = load_signals(...)
        
    def on_start(self) -> None:
        self.write_log("Portfolio Strategy Started")

    def on_stop(self) -> None:
        self.write_log("Portfolio Strategy Stopped")

    def on_tick(self, tick: TickData) -> None:
        pass

    def on_bars(self, bars: Dict[str, BarData]) -> None:
        # Update ArrayManagers
        for vt_symbol, bar in bars.items():
            am = self.ams[vt_symbol]
            am.update_bar(bar)
            
        # Ensure we have enough data
        if not all(am.inited for am in self.ams.values()):
            return
            
        current_dt = list(bars.values())[0].datetime.date()
        
        # 1. Check Rebalance
        if self.check_rebalance(current_dt):
            self.rebalance(bars, current_dt)
            self.last_rebalance_date = current_dt
            
        # 2. Check Stop Loss (Daily)
        self.check_stop_loss(bars)

    def check_rebalance(self, current_dt) -> bool:
        if self.last_rebalance_date is None:
            return True
        
        # Simple day count check (approximate)
        days_since = (current_dt - self.last_rebalance_date).days
        if days_since >= self.rebalance_interval:
             return True
        return False

    def rebalance(self, bars: Dict[str, BarData], current_dt):
        self.write_log(f"Rebalancing on {current_dt}")
        
        # Prepare Price History
        history_data = {vt_symbol: am.close for vt_symbol, am in self.ams.items()}
        price_df = pd.DataFrame(history_data)
        # Ensure index is datetime for optimization functions
        # ArrayManager doesn't store dates, so we might need to reconstruct or use a different data loader
        # For this template, we assume price_df is sufficient for volatility calc, 
        # but for WFO we ideally need dates. 
        # NOTE: In a real implementation, use a Datafeed to fetch historical DF with dates.
        
        returns_df = price_df.pct_change().dropna()
        
        # --- Dynamic Parameter Optimization ---
        # 1. Optimize Top-K
        # (Requires signal_df to be populated)
        if not self.signals.empty:
            self.current_k = optimize_top_k(
                self.signals, 
                price_df, # Note: This needs date index to work correctly with WFO
                current_dt,
                lookback_months=3,
                candidates=[5, 10, 20]
            )
            self.write_log(f"Optimized Top-K: {self.current_k}")
        
        # 2. Optimize Stop Loss
        market_vol = returns_df.mean(axis=1).std() * np.sqrt(252)
        self.current_stop_loss = optimize_stop_loss(market_vol, self.base_stop_loss)
        self.write_log(f"Optimized Stop-Loss: {self.current_stop_loss:.2%}")
        
        # --- Weighting Strategy ---
        # Select Top-K symbols based on latest signal
        # Mock selection for template:
        top_k_symbols = self.vt_symbols[:self.current_k] 
        
        selected_returns = returns_df[top_k_symbols]
        
        if self.use_hrp:
            weights = calculate_hrp_weights(selected_returns)
        else:
            weights = calculate_inverse_volatility_weights(selected_returns)
            
        # --- Execution ---
        for vt_symbol in self.vt_symbols:
            if vt_symbol in weights:
                target_weight = weights[vt_symbol]
                price = bars[vt_symbol].close_price
                if price > 0:
                    target_volume = int((self.capital * target_weight) / price)
                    self.set_target(vt_symbol, target_volume)
            else:
                self.set_target(vt_symbol, 0)
                
        self.rebalance_portfolio(bars)

    def check_stop_loss(self, bars):
        # Implement trailing stop or fixed stop loss logic here
        pass