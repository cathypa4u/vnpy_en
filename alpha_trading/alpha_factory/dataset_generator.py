"""
Layer 1: Alpha Factory - Step 2: Dataset Generation (Improved)

Uses the custom AdaptiveDataset to perform feature engineering and saves
the processed datasets for each model type (MLP, LGBM, Lasso).

IMPROVEMENTS:
- Now loads hourly data and passes it to the dataset's prepare_data method.
"""
from datetime import datetime
from functools import partial
import polars as pl

from vnpy.alpha.lab import AlphaLab
from vnpy.alpha.dataset import (
    process_fill_na,
    process_robust_zscore_norm,
    process_cs_rank_norm,
    process_drop_na
)
from vnpy.trader.constant import Interval

from alpha_trading.alpha_factory.adaptive_dataset import AdaptiveDataset
from alpha_trading.alpha_factory.setup_lab import LAB_NAME, INDEX_SYMBOL, START_DATE, END_DATE, HOURLY_DATA_YEARS

# --- Configuration ---
DATASET_NAME = "adaptive"
TRAIN_PERIOD = ("2015-01-01", "2022-12-31")
TEST_PERIOD = ("2023-01-01", "2025-12-31")
EXTENDED_DAYS = 252 * 2 # Load extra 2 years of data for feature calculation buffer

def generate_datasets():
    """
    Generates and saves preprocessed datasets for different models.
    """
    print("--- Alpha Factory: Step 2: Dataset Generation (Improved) ---")
    
    # 1. Initialize AlphaLab
    lab = AlphaLab(LAB_NAME)

    # 2. Load raw daily and hourly data
    print("Loading raw daily and hourly data...")
    symbols = lab.load_component_symbols(INDEX_SYMBOL, START_DATE, END_DATE)
    
    df_daily = lab.load_bar_df(symbols, Interval.DAILY, START_DATE, END_DATE, EXTENDED_DAYS)
    
    start_hourly = END_DATE - timedelta(days=365 * HOURLY_DATA_YEARS)
    hourly_bars = lab.load_bar_data(symbols, Interval.HOUR, start_hourly, END_DATE)
    df_hourly = pl.from_dicts([bar.__dict__ for bar in hourly_bars]) if hourly_bars else pl.DataFrame()

    # 3. Initialize AdaptiveDataset and run feature engineering
    dataset = AdaptiveDataset(
        df=df_daily,
        train_period=TRAIN_PERIOD,
        test_period=TEST_PERIOD,
    )
    dataset.prepare_data(hourly_df=df_hourly)
    lab.save_dataset(DATASET_NAME, dataset)
    print(f"Saved raw '{DATASET_NAME}' dataset.")

    # 4. Create and save model-specific processed datasets
    
    # MLP Dataset
    print("\nCreating MLP-specific dataset...")
    mlp_dataset = lab.load_dataset(DATASET_NAME)
    fit_start_time = datetime.strptime(TRAIN_PERIOD[0], "%Y-%m-%d")
    fit_end_time = datetime.strptime(TRAIN_PERIOD[1], "%Y-%m-%d")
    
    mlp_dataset.add_processor("infer", partial(process_robust_zscore_norm, fit_start_time=fit_start_time, fit_end_time=fit_end_time))
    mlp_dataset.add_processor("infer", partial(process_fill_na, fill_value=0, fill_label=False))
    mlp_dataset.add_processor("learn", partial(process_drop_na, names=["label"]))
    mlp_dataset.add_processor("learn", partial(process_cs_rank_norm, names=["label"]))
    mlp_dataset.add_processor("learn", partial(process_fill_na, fill_value=0, fill_label=True))
    mlp_dataset.process_data()
    lab.save_dataset(f"{DATASET_NAME}_mlp", mlp_dataset)
    print(f"Saved '{DATASET_NAME}_mlp' dataset.")
    
    # LGBM Dataset
    print("\nCreating LGBM-specific dataset...")
    lgbm_dataset = lab.load_dataset(DATASET_NAME)
    lgbm_dataset.add_processor("learn", partial(process_drop_na, names=["label"]))
    lgbm_dataset.process_data()
    lab.save_dataset(f"{DATASET_NAME}_lgbm", lgbm_dataset)
    print(f"Saved '{DATASET_NAME}_lgbm' dataset.")

    print("\nDataset generation complete.")
    print("Next step: Run `model_trainer.py` to train models.")

if __name__ == "__main__":
    from datetime import timedelta # Add this import
    generate_datasets()