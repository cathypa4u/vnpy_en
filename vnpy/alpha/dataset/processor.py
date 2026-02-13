from datetime import datetime

import numpy as np
import polars as pl

from .utility import to_datetime


def process_drop_na(df: pl.DataFrame, names: list[str] | None = None) -> pl.DataFrame:
    """Remove rows with missing values"""
    if names is None:
        names = df.columns[2:-1]

    for name in names:
        df = df.with_columns(
            pl.col(name).fill_nan(None)
        )
    df = df.drop_nulls(subset=names)
    return df


def process_fill_na(df: pl.DataFrame, fill_value: float, fill_label: bool = True) -> pl.DataFrame:
    """Fill missing values"""
    if fill_label:
        df = df.fill_null(fill_value)
        df = df.fill_nan(fill_value)
    else:
        df = df.with_columns(
            [pl.col(col).fill_null(fill_value).fill_nan(fill_value) for col in df.columns[2:-1]]
        )
    return df

def process_inf_to_zero(df: pl.DataFrame, names: list[str] | None = None) -> pl.DataFrame:
    """데이터프레임 내의 무한대(Inf) 값을 0으로 치환"""
    # 컬럼명이 지정되지 않았다면 datetime, vt_symbol을 제외한 모든 수치 컬럼 대상
    if names is None:
        names = [col for col in df.columns if col not in ["datetime", "vt_symbol"]]

    # 각 컬럼에 대해 무한대 확인 및 치환
    return df.with_columns([
        pl.when(pl.col(name).is_infinite())
        .then(0.0)
        .otherwise(pl.col(name))
        .alias(name)
        for name in names
    ])

def process_cs_norm(
    df: pl.DataFrame,
    names: list[str],
    method: str         # robust/zscore
) -> pl.DataFrame:
    """Cross-sectional normalization"""
    _df: pl.DataFrame = df.fill_nan(None)

    # Median method
    if method == "robust":
        for col in names:
            df = df.with_columns(
                _df.select(
                    (pl.col(col) - pl.col(col).median()).over("datetime").alias(col),
                )
            )

            df = df.with_columns(
                df.select(
                    pl.col(col).abs().median().over("datetime").alias("mad"),
                )
            )

            df = df.with_columns(
                (pl.col(col) / pl.col("mad") / 1.4826).clip(-3, 3).alias(col)
            ).drop(["mad"])
    # Z-Score method
    else:
        for col in names:
            df = df.with_columns(
                _df.select(
                    pl.col(col).mean().over("datetime").alias("mean"),
                    pl.col(col).std().over("datetime").alias("std"),
                )
            )

            df = df.with_columns(
                (pl.col(col) - pl.col("mean")) / pl.col("std").alias(col)
            ).drop(["mean", "std"])

    return df


def process_robust_zscore_norm(
    df: pl.DataFrame,
    fit_start_time: datetime | str | None = None,
    fit_end_time: datetime | str | None = None,
    clip_outlier: bool = True
) -> pl.DataFrame:
    """Robust Z-Score normalization"""
    _df: pl.DataFrame = df.fill_nan(None)

    if fit_start_time and fit_end_time:
        fit_start_time = to_datetime(fit_start_time)
        fit_end_time = to_datetime(fit_end_time)
        _df = _df.filter((pl.col("datetime") >= fit_start_time) & (pl.col("datetime") <= fit_end_time))

    cols = df.columns[2:-1]
    X = _df.select(cols).to_numpy()

    mean_train = np.nanmedian(X, axis=0)
    std_train = np.nanmedian(np.abs(X - mean_train), axis=0)
    std_train += 1e-12
    std_train *= 1.4826

    for name in cols:
        normalized_col = (
            (pl.col(name) - mean_train[cols.index(name)]) / std_train[cols.index(name)]
        ).cast(pl.Float64)

        if clip_outlier:
            normalized_col = normalized_col.clip(-3, 3)

        df = df.with_columns(normalized_col.alias(name))

    return df


def process_cs_rank_norm(df: pl.DataFrame, names: list[str]) -> pl.DataFrame:
    """Cross-sectional rank normalization"""
    _df: pl.DataFrame = df.fill_nan(None)

    _df = _df.with_columns([
        ((pl.col(col).rank("average").over("datetime") / pl.col("datetime").count().over("datetime")) - 0.5) * 3.46
        for col in names
    ])

    df = df.with_columns([
        _df[col].alias(col) for col in names
    ])

    return df
