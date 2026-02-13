"""
Layer 1: Alpha Factory - Custom Adaptive Dataset (Improved)

Defines a custom AlphaDataset that includes all required features:
- Alpha 101 & 158
- Regime Features (ADX, MA Disparity)
- Intraday Fusion Features

IMPROVEMENTS:
- Decoupled from AlphaLab: The class no longer holds an instance of AlphaLab.
- Encapsulation: Hourly data is now passed into prepare_data instead of being loaded internally.
- Idiomatic Pattern: Follows the vnpy pattern more closely by preparing an enriched
  DataFrame and then explicitly defining feature_expressions from its columns.
"""
import polars as pl

from vnpy.alpha.dataset import AlphaDataset
from vnpy.alpha.dataset.datasets.alpha_101 import Alpha101
from vnpy.alpha.dataset.datasets.alpha_158 import Alpha158

class AdaptiveDataset(AlphaDataset):
    """
    A custom dataset that generates a combination of alpha factors,
    regime features, and intraday features.
    """
        def prepare_data(self, hourly_df: pl.DataFrame = None, *args, **kwargs):
            """
            Overrides the base method to perform all feature engineering steps.
            
            :param hourly_df: A Polars DataFrame containing hourly bar data.
            """
            print("--- Preparing AdaptiveDataset (Corrected) ---")
            
            # Step 1: Eagerly compute complex custom features first
            print("Step 1: Calculating custom features (Regime, Intraday)...")
            enriched_df = self.df
            enriched_df = self._add_regime_features(enriched_df)
            if hourly_df is not None and not hourly_df.is_empty():
                enriched_df = self._add_intraday_features(enriched_df, hourly_df)
    
            # Update the internal dataframe so that the alpha expressions operate on the enriched data
            self.df = enriched_df.fill_nan(0).fill_null(0)
    
            # Step 2: "Borrow" the lazy feature expressions from Alpha101 and Alpha158
            print("Step 2: Collecting feature expressions from Alpha101 & Alpha158...")
            alpha101_expressions = Alpha101(df=self.df).feature_expressions
            alpha158_expressions = Alpha158(df=self.df).feature_expressions
            
            # Step 3: Combine all feature expressions
            print("Step 3: Combining all feature sources...")
            final_feature_expressions = {}
            final_feature_expressions.update(alpha101_expressions)
            final_feature_expressions.update(alpha158_expressions)
    
            # Add the eagerly computed features as simple column selections
            custom_feature_names = ["adx", "ma_disparity", "intraday_vol_pct", "vol_am_pm_ratio"]
            for name in custom_feature_names:
                if name in self.df.columns:
                    final_feature_expressions[name] = pl.col(name)
    
            # Step 4: Set the final expressions for the dataset
            self.feature_expressions = final_feature_expressions
            self.label_expression = (
                pl.col("close").shift(-20) / pl.col("close") - 1
            ).over("vt_symbol")
            
            print(f"--- AdaptiveDataset preparation complete with {len(self.feature_expressions)} total features ---")

    def _add_regime_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates and adds ADX and MA Disparity."""
        df_adx = df.group_by("vt_symbol", maintain_order=True).apply(self._calculate_adx_per_symbol)
        
        ma5 = df.group_by("vt_symbol", maintain_order=True).agg(pl.col("close").rolling_mean(5).alias("ma5"))["ma5"]
        ma60 = df.group_by("vt_symbol", maintain_order=True).agg(pl.col("close").rolling_mean(60).alias("ma60"))["ma60"]
        disparity = ((ma5 - ma60) / ma60).alias("ma_disparity")

        return df_adx.with_columns(disparity)

    @staticmethod
    def _calculate_adx_per_symbol(df: pl.DataFrame, period: int = 14) -> pl.DataFrame:
        """Calculates ADX for a single symbol's dataframe."""
        df = df.sort("datetime")
        high_low = df["high"] - df["low"]
        high_prev_close = (df["high"] - df["close"].shift(1)).abs()
        low_prev_close = (df["low"] - df["close"].shift(1)).abs()
        
        tr = pl.max_horizontal([high_low, high_prev_close, low_prev_close])
        atr = tr.ewm_mean(span=period, adjust=False)
        
        move_up = df["high"] - df["high"].shift(1)
        move_down = df["low"].shift(1) - df["low"]
        
        dm_plus = pl.when((move_up > move_down) & (move_up > 0)).then(move_up).otherwise(0.0)
        dm_minus = pl.when((move_down > move_up) & (move_down > 0)).then(move_down).otherwise(0.0)

        di_plus = (dm_plus.ewm_mean(span=period, adjust=False) / atr) * 100
        di_minus = (dm_minus.ewm_mean(span=period, adjust=False) / atr) * 100
        
        dx = ((di_plus - di_minus).abs() / (di_plus + di_minus).abs()) * 100
        adx = dx.ewm_mean(span=period, adjust=False).alias("adx")
        
        return df.with_columns(adx)

    def _add_intraday_features(self, df: pl.DataFrame, hourly_df: pl.DataFrame) -> pl.DataFrame:
        """Calculates and adds features from hourly data."""
        hourly_df = hourly_df.with_columns(pl.col("datetime").dt.hour().alias("hour"))
        
        # Calculate intraday features
        daily_agg = hourly_df.group_by("vt_symbol", pl.col("datetime").dt.date().alias("date")).agg(
            ((pl.max("high") - pl.min("low")) / pl.first("open")).alias("intraday_vol_pct"),
            pl.sum("volume").filter(pl.col("hour").is_between(9, 11)).alias("morning_vol"),
            pl.sum("volume").filter(pl.col("hour").is_between(12, 15)).alias("afternoon_vol")
        )
        
        daily_agg = daily_agg.with_columns(
            (pl.col("morning_vol") / pl.col("afternoon_vol")).alias("vol_am_pm_ratio")
        )
        
        # Join with the main daily dataframe
        df = df.join(
            daily_agg.select(["date", "vt_symbol", "intraday_vol_pct", "vol_am_pm_ratio"]),
            left_on=[pl.col('datetime').dt.date(), "vt_symbol"],
            right_on=["date", "vt_symbol"],
            how="left"
        )
        return df