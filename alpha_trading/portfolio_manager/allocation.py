"""
Portfolio Allocation Strategies

Implements weighting strategies for portfolio construction:
1. Inverse Volatility (Mandatory)
2. Hierarchical Risk Parity (HRP) (Advanced)
"""

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, leaves_list
from scipy.spatial.distance import squareform

def calculate_inverse_volatility_weights(returns: pd.DataFrame) -> pd.Series:
    """
    Calculate weights based on Inverse Volatility.
    
    Formula:
        W_i = (1 / sigma_i) / sum(1 / sigma_j)
        
    Args:
        returns: DataFrame of asset returns
        
    Returns:
        Series of weights summing to 1.0
    """
    # Calculate annualized volatility (assuming daily returns)
    # We use standard deviation of the provided returns window
    volatility = returns.std()
    
    # Handle zero volatility to avoid division by zero
    volatility = volatility.replace(0, np.inf)
    
    inv_volatility = 1 / volatility
    weights = inv_volatility / inv_volatility.sum()
    
    return weights.fillna(0)

def calculate_hrp_weights(returns: pd.DataFrame) -> pd.Series:
    """
    Calculate weights using Hierarchical Risk Parity (HRP).
    Clusters correlated stocks to prevent sector concentration.
    """
    # 1. Correlation and Covariance
    corr = returns.corr().fillna(0)
    cov = returns.cov().fillna(0)
    
    # 2. Hierarchical Clustering
    # Calculate distance matrix: sqrt(0.5 * (1 - rho))
    dist = np.sqrt((1 - corr) / 2)
    dist = dist.fillna(0)
    np.fill_diagonal(dist.values, 0)
    
    # Linkage and Quasi-Diagonalization
    condensed_dist = squareform(dist)
    link = linkage(condensed_dist, method='single')
    sort_ix = leaves_list(link)
    ordered_tickers = corr.index[sort_ix].tolist()
    
    # 3. Recursive Bisection
    weights = _get_recursive_bisection_weights(cov, ordered_tickers)
    return weights

def _get_recursive_bisection_weights(cov: pd.DataFrame, tickers: list) -> pd.Series:
    """Helper for HRP: Recursively split clusters and allocate risk."""
    weights = pd.Series(1.0, index=tickers)
    
    def recurse(sub_tickers):
        if len(sub_tickers) <= 1:
            return
        
        split = len(sub_tickers) // 2
        left = sub_tickers[:split]
        right = sub_tickers[split:]
        
        cov_left = cov.loc[left, left]
        cov_right = cov.loc[right, right]
        
        # Calculate cluster variance using Inverse Variance Portfolio (IVP) assumption within cluster
        # Var_cluster = w_ivp.T * Cov * w_ivp
        
        def get_cluster_var(c):
            ivp = 1 / np.diag(c)
            ivp /= ivp.sum()
            return np.dot(np.dot(ivp, c), ivp)
            
        var_left = get_cluster_var(cov_left)
        var_right = get_cluster_var(cov_right)
        
        # Allocation factor alpha
        # Allocate more to the cluster with lower variance
        alpha = 1 - var_left / (var_left + var_right)
        
        weights[left] *= alpha
        weights[right] *= (1 - alpha)
        
        recurse(left)
        recurse(right)
        
    recurse(tickers)
    return weights