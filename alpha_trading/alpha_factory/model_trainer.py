"""
Layer 1: Alpha Factory - Step 3: Model Trainer (Improved)

Implements the Rolling Window and Sample Weighting training methodology.

IMPROVEMENTS:
- Robust Sample Weighting: Generates weights *after* fetching processed
  training data, ensuring perfect alignment and preventing errors from
  dropped NaN values.
"""
import numpy as np
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import torch

from vnpy.alpha.lab import AlphaLab
from vnpy.alpha.dataset import AlphaDataset

from alpha_trading.alpha_factory.setup_lab import LAB_NAME
from alpha_trading.alpha_factory.dataset_generator import DATASET_NAME
from alpha_trading.alpha_factory.custom_models import WeightedLgbModel, WeightedMlpModel

# --- Configuration ---
TRAIN_WINDOW_YEARS = 5
RECENT_DATA_MONTHS = 6
RECENT_DATA_WEIGHT = 2.0

ROLLING_START_DATE = datetime(2022, 1, 1)
ROLLING_END_DATE = datetime(2024, 1, 1)

def get_aligned_sample_weights(
    X_learn: pl.DataFrame,
    train_end: datetime,
    recent_months: int,
    weight: float
) -> np.ndarray:
    """
    Generates sample weights that are perfectly aligned with the input data.
    
    :param X_learn: The feature DataFrame returned by dataset.fetch_learn().
                    It MUST contain the 'datetime' column.
    """
    datetimes = X_learn["datetime"]
    cutoff_date = train_end - relativedelta(months=recent_months)
    
    # Use Polars for efficient conditional weighting
    weights_series = pl.when(datetimes > cutoff_date).then(weight).otherwise(1.0)
    
    return weights_series.to_numpy()

def rolling_window_trainer():
    """
    Orchestrates the rolling window training process with robust sample weighting.
    """
    print("--- Alpha Factory: Step 3: Rolling Window Model Trainer (Improved) ---")
    
    lab = AlphaLab(LAB_NAME)
    
    models_to_train = {
        "mlp": WeightedMlpModel,
        "lgbm": WeightedLgbModel,
    }

    current_date = ROLLING_START_DATE
    while current_date <= ROLLING_END_DATE:
        train_end = current_date
        train_start = train_end - relativedelta(years=TRAIN_WINDOW_YEARS)
        model_timestamp = train_end.strftime("%Y%m")
        
        print(f"\n--- Training models for month-end: {train_end.strftime('%Y-%m')} ---")
        print(f"Training data window: {train_start.date()} to {train_end.date()}")

        for model_type, ModelClass in models_to_train.items():
            print(f"  Training {model_type.upper()} model...")
            
            # 1. Load dataset and set the training period for this window
            dataset: AlphaDataset = lab.load_dataset(f"{DATASET_NAME}_{model_type}")
            dataset.train_period = (train_start.strftime("%Y-%m-%d"), train_end.strftime("%Y-%m-%d"))

            # 2. Fetch processed training data FIRST
            X_learn_df, y_learn = dataset.fetch_learn()
            
            # 3. Generate sample weights based on the ALIGNED data
            sample_weights = get_aligned_sample_weights(
                X_learn_df, train_end, RECENT_DATA_MONTHS, RECENT_DATA_WEIGHT
            )
            print(f"  Data points for training: {len(y_learn)}, aligned weights generated.")

            # 4. Initialize and train the weighted model
            if model_type == "mlp":
                device = "cuda" if torch.cuda.is_available() else "cpu"
                model = ModelClass(
                    input_size=len(dataset.feature_expressions),
                    device=device,
                    n_epochs=50 # Using fewer epochs for faster rolling training
                )
            else:
                model = ModelClass()
            
            # The custom fit method expects the dataset object
            # The model's internal `fit` will call `fetch_learn` again, but this is
            # a necessary step in the vnpy framework. We pass our aligned weights.
            model.fit(dataset, sample_weight=sample_weights)
            
            # 5. Save the trained model with a timestamp
            model_name = f"{DATASET_NAME}_{model_type}_{model_timestamp}"
            lab.save_model(model_name, model)
            print(f"  Saved model: {model_name}")

        current_date += relativedelta(months=1)
        
    print("\nRolling window training complete.")
    print("Next step: Run `signal_generator.py` to generate daily signals.")

if __name__ == "__main__":
    rolling_window_trainer()