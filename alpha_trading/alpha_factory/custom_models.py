"""
Layer 1: Alpha Factory - Custom Models for Weighted Training

Extends vnpy's base AlphaModels to support sample weighting,
which is crucial for the Hybrid Training methodology.
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from tqdm import tqdm

from vnpy.alpha.model.models.lgb_model import LgbModel
from vnpy.alpha.model.models.mlp_model import MlpModel
from vnpy.alpha.dataset import AlphaDataset

class WeightedLgbModel(LgbModel):
    """
    Custom LightGBM model that supports sample weighting during training.
    """
    def fit(self, dataset: AlphaDataset, sample_weight: np.ndarray = None, **kwargs):
        """
        Overrides the base fit method to accept sample weights.
        """
        X, y = dataset.fetch_learn()
        
        if sample_weight is not None:
            print("Fitting LightGBM with sample weights.")
        
        self.model.fit(X, y, sample_weight=sample_weight, **kwargs)
        self.trained = True

class WeightedMlpModel(MlpModel):
    """
    Custom MLP model that supports sample weighting via a weighted loss function.
    """
    def fit(self, dataset: AlphaDataset, sample_weight: np.ndarray = None):
        """
        Overrides the base fit method to use a weighted MSE loss.
        """
        X, y = dataset.fetch_learn()
        
        X_tensor = torch.tensor(X, dtype=torch.float32)
        y_tensor = torch.tensor(y.reshape(-1, 1), dtype=torch.float32)

        if sample_weight is None:
            # If no weights, use array of ones
            sample_weight = np.ones(len(X))
        
        weights_tensor = torch.tensor(sample_weight.reshape(-1, 1), dtype=torch.float32)
        
        torch_dataset = TensorDataset(X_tensor, y_tensor, weights_tensor)
        loader = DataLoader(torch_dataset, batch_size=self.batch_size, shuffle=True)
        
        # Define a weighted loss function
        def weighted_mse_loss(output, target, weight):
            return torch.mean(weight * (output - target) ** 2)
            
        self.model.to(self.device)
        self.model.train()
        
        print("Fitting MLP with sample weights.")
        for i in tqdm(range(self.n_epochs), desc="MLP Epochs"):
            for X_batch, y_batch, w_batch in loader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device)
                w_batch = w_batch.to(self.device)
                
                self.optimizer.zero_grad()
                outputs = self.model(X_batch)
                
                loss = weighted_mse_loss(outputs, y_batch, w_batch)
                
                loss.backward()
                self.optimizer.step()
        
        self.trained = True
