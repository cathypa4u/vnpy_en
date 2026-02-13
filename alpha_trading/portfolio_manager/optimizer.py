# Layer 2: Portfolio Manager (Portfolio Strategy)
# The Fund Manager

"""
Portfolio Weighting and Optimization Algorithms
- Inverse Volatility Weighting
- Hierarchical Risk Parity (HRP)
"""

def calculate_inverse_volatility_weights(alpha_scores):
    """
    Calculates weights based on the inverse of volatility.
    Wi = (1 / sigma_i) / sum(1 / sigma_j)
    """
    print("Calculating Inverse Volatility weights...")
    # 종목별 변동성 계산
    # 역변동성 가중치 계산
    return "inv_vol_weights"

def calculate_hrp_weights(alpha_scores):
    """
    Calculates weights using Hierarchical Risk Parity.
    Clusters highly correlated assets to prevent sector concentration.
    """
    print("Calculating Hierarchical Risk Parity (HRP) weights...")
    # 상관관계 매트릭스 계산
    # HRP 알고리즘 적용하여 가중치 계산
    return "hrp_weights"
