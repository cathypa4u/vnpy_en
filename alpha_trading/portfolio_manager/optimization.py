"""
Dynamic Parameter Optimization (Walk-Forward)

Implements logic for:
1. Dynamic Top-K Selection
2. Dynamic Stop-Loss Adjustment
"""

import numpy as np
import pandas as pd
from datetime import timedelta

def optimize_top_k(
    signal_df: pd.DataFrame, 
    price_df: pd.DataFrame, 
    current_date, 
    lookback_months: int = 3, 
    candidates: list = [5, 10, 20]
) -> int:
    """
    Determine the optimal number of positions (K) based on recent simulation results.
    
    Args:
        signal_df: DataFrame with columns ['datetime', 'vt_symbol', 'signal'] or pivoted.
        price_df: DataFrame of close prices (index=datetime, columns=vt_symbol).
        current_date: The date for which we are optimizing.
        lookback_months: Period to simulate (default 3 months).
        candidates: List of K values to test.
        
    Returns:
        Best K value.
    """
    # Pivot signal_df if it's in long format
    if 'vt_symbol' in signal_df.columns and 'signal' in signal_df.columns:
        # Ensure datetime is datetime object
        if not pd.api.types.is_datetime64_any_dtype(signal_df['datetime']):
            signal_df['datetime'] = pd.to_datetime(signal_df['datetime'])
        signal_df = signal_df.pivot(index='datetime', columns='vt_symbol', values='signal')
    
    # Define lookback period
    current_dt = pd.to_datetime(current_date)
    start_date = current_dt - timedelta(days=lookback_months * 30)
    
    # Calculate daily returns
    daily_returns = price_df.pct_change().fillna(0)
    
    # Filter data for simulation period
    mask = (daily_returns.index >= start_date) & (daily_returns.index < current_dt)
    period_returns = daily_returns.loc[mask]
    
    if period_returns.empty:
        return 10 # Default fallback
        
    best_k = 10
    best_sharpe = -np.inf
    
    for k in candidates:
        strategy_returns = []
        
        # Simple vector backtest
        for dt in period_returns.index:
            # Use signal from previous day (or same day if signals are generated before open)
            # Here we assume signal_df is aligned such that signal at T is used for T's return
            if dt not in signal_df.index:
                strategy_returns.append(0.0)
                continue
                
            day_signals = signal_df.loc[dt]
            top_k_tickers = day_signals.nlargest(k).index
            
            # Calculate equal-weighted return of Top-K
            valid_tickers = [t for t in top_k_tickers if t in period_returns.columns]
            if valid_tickers:
                day_ret = period_returns.loc[dt, valid_tickers].mean()
            else:
                day_ret = 0.0
                
            strategy_returns.append(day_ret)
            
        if not strategy_returns:
            continue
            
        mean_ret = np.mean(strategy_returns)
        std_ret = np.std(strategy_returns)
        sharpe = mean_ret / std_ret if std_ret > 0 else 0
        
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_k = k
            
    return best_k

def optimize_stop_loss(market_volatility: float, base_sl: float = 0.05) -> float:
    """
    Adjust stop-loss based on market volatility.
    """
    if market_volatility > 0.40:  # High volatility -> Wider stop
        return base_sl * 1.5
    elif market_volatility < 0.15: # Low volatility -> Tighter stop
        return base_sl * 0.8
    else:
        return base_sl