# Main Scheduler
# This script runs the entire daily trading cycle sequentially.

import alpha_trading.1_alpha_factory.signal_gen as alpha_factory
import alpha_trading.2_portfolio_manager.allocator as portfolio_manager
import alpha_trading.3_execution_algo.algo_trader as execution_algo

def run_weekend_maintenance():
    """
    Run weekly/monthly data updates and model retraining.
    """
    print("=== STARTING WEEKEND MAINTENANCE ===")
    # 1. 데이터 업데이트 (주가, 거시지표)
    # 2. Rolling Retraining & Model Save
    # 3. Walk-forward-Optimization for parameters
    print("=== WEEKEND MAINTENANCE COMPLETE ===")


def run_daily_cycle():
    """
    Run the daily trading workflow.
    """
    print("=== STARTING DAILY TRADING CYCLE ===")
    
    # [D-day 15:40] After market close
    print("
[Step 1/3] Generating Alpha Signals for next day...")
    # alpha_factory.run_inference()
    # alpha_factory.dynamic_ensemble()
    # alpha_factory.save_signals_to_csv()
    print("Alpha signals generated.")
    
    # [D+1 08:30] Before market open
    print("
[Step 2/3] Constructing Target Portfolio...")
    # portfolio_manager.calculate_target_quantities()
    # portfolio_manager.save_targets_to_json()
    print("Target portfolio constructed.")

    # [D+1 09:00-15:30] During market hours
    print("
[Step 3/3] Starting Execution Algorithm...")
    # trader = execution_algo.AlgoTrader(vnpy_engine)
    # trader.run()
    print("Execution finished.")

    print("
=== DAILY TRADING CYCLE COMPLETE ===")


if __name__ == "__main__":
    # This would be triggered by a scheduler (e.g., cron)
    # For example, on weekdays, run daily cycle. On weekends, run maintenance.
    import datetime
    today = datetime.datetime.today().weekday()

    if today < 5: # Monday to Friday
        run_daily_cycle()
    else: # Saturday or Sunday
        run_weekend_maintenance()
