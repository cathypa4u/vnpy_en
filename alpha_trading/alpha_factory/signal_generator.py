"""
Layer 1: Alpha Factory - Step 4: Daily Signal Generator

Performs the final step of the Alpha Factory: Dynamic Ensemble.
- Loads the latest models for the given prediction date.
- Calculates recent performance (IC) for each model.
- Generates a weighted-average signal based on IC.
- Saves the final signal for the portfolio manager.
"""
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import torch

from vnpy.alpha.lab import AlphaLab
from vnpy.alpha.dataset import AlphaDataset, Segment

from alpha_trading.alpha_factory.setup_lab import LAB_NAME
from alpha_trading.alpha_factory.dataset_generator import DATASET_NAME

# --- Configuration ---
# The date for which to generate signals
PREDICTION_DATE = datetime.now()
# PREDICTION_DATE = datetime(2024, 2, 1) # For testing

IC_CALCULATION_PERIOD = ("1m", "1-month") # vnpy period tuple for IC calculation
FINAL_SIGNAL_NAME = "adaptive_ensemble"

def generate_ensemble_signal(prediction_date: datetime):
    """
    Orchestrates the daily signal generation and dynamic ensemble process.
    """
    print(f"--- Alpha Factory: Step 4: Dynamic Signal Generation for {prediction_date.date()} ---")
    
    lab = AlphaLab(LAB_NAME)
    
    # 1. Identify and load the latest set of trained models
    # Models are trained at month-end, so for any day in month M, use models from M-1.
    model_timestamp = (prediction_date - relativedelta(months=1)).strftime("%Y%m")
    model_types = ["mlp", "lgbm"] # Add "lasso" if it was trained
    
    models = {}
    print(f"Loading models for timestamp: {model_timestamp}")
    for m_type in model_types:
        model_name = f"{DATASET_NAME}_{m_type}_{model_timestamp}"
        try:
            models[m_type] = lab.load_model(model_name)
            print(f"  Loaded {model_name}")
        except FileNotFoundError:
            print(f"  Warning: Model {model_name} not found. Skipping.")
    
    if not models:
        print("Error: No models found. Cannot generate signals.")
        return

    # 2. Calculate IC-based weights for the ensemble
    model_weights = {}
    print("Calculating dynamic weights based on recent IC...")

    for model_type, model in models.items():
        dataset: AlphaDataset = lab.load_dataset(f"{DATASET_NAME}_{model_type}")

        # Set the test_period to the last ~2 months to calculate performance
        ic_start_date = prediction_date - relativedelta(months=2)
        dataset.test_period = (ic_start_date.strftime("%Y-%m-%d"), prediction_date.strftime("%Y-%m-%d"))

        signal = model.predict(dataset, Segment.TEST)

        # Use vnpy's built-in performance calculation
        perf = dataset.get_signal_performance(signal, period=IC_CALCULATION_PERIOD[0])

        if perf is None or "ic" not in perf or perf["ic"].is_empty():
            ic = 0.0
            print(f"  Could not calculate IC for {model_type}. Defaulting to 0.")
        else:
            ic = perf["ic"].item()
            print(f"  {model_type.upper()} IC ({IC_CALCULATION_PERIOD[1]}): {ic:.4f}")

        model_weights[model_type] = abs(ic)  # Use absolute IC for weighting

    # Normalize weights
    total_ic = sum(model_weights.values())
    if total_ic > 0:
        model_weights = {m: w / total_ic for m, w in model_weights.items()}
    else:
        print("Warning: All model ICs are zero. Using equal weights.")
        model_weights = {m: 1.0 / len(models) for m in models.keys()}

    print("Ensemble Weights:")
    for m, w in model_weights.items():
        print(f"  - {m.upper()}: {w:.2%}")

    # 3. Generate final ensembled signal for the prediction date
    final_signal_df = None
    
    print("Generating final ensembled signal...")
    for model_type, model in models.items():
        dataset: AlphaDataset = lab.load_dataset(f"{DATASET_NAME}_{model_type}")

        # Set test_period to the single day
        dataset.test_period = (prediction_date.strftime("%Y-%m-%d"), prediction_date.strftime("%Y-%m-%d"))

        # Generate signal for the specific date
        signal_array = model.predict(dataset, Segment.TEST)
        signal_df = dataset.fetch_predict(Segment.TEST)
        signal_df = signal_df.with_columns(pl.Series("signal", signal_array))

        # Weight the signal
        weighted_signal = signal_df["signal"] * model_weights[model_type]
        
        if final_signal_df is None:
            final_signal_df = signal_df.select(["datetime", "vt_symbol"])
            final_signal_df = final_signal_df.with_columns(weighted_signal.alias("final_signal"))
        else:
            final_signal_df = final_signal_df.with_columns(
                pl.col("final_signal") + weighted_signal
            )

    # 4. Save the final signal
    if final_signal_df is not None:
        # Rename column for consistency
        final_signal_df = final_signal_df.rename({"final_signal": "signal"})
        
        lab.save_signal(FINAL_SIGNAL_NAME, final_signal_df)
        print(f"
Successfully saved final signal '{FINAL_SIGNAL_NAME}'.")
        print(final_signal_df.head())
    else:
        print("Error: Could not generate final signal.")

if __name__ == "__main__":
    generate_ensemble_signal(PREDICTION_DATE)
