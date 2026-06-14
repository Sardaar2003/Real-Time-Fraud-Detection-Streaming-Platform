#!/usr/bin/env python3
"""
Model Training Script - Real-Time Fraud Detection Streaming Platform

Generates synthetic historical transactions mimicking the real-time feature set,
trains an XGBoost binary classifier, evaluates performance, and exports
the model artifact as 'model.bst' for Vertex AI Endpoint hosting.
"""

import os
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix

def synthesize_training_data(n_samples: int = 20000) -> pd.DataFrame:
    """
    Generates synthetic transaction feature records for training.
    Injects three types of fraudulent profiles:
    1. High Amount Anomaly
    2. Impossible Travel Velocity
    3. High Frequency Transaction Burst
    """
    print(f"Generating {n_samples} synthetic transaction records...")
    
    # Feature columns: amount, tx_count_10m, tx_sum_10m, impossible_travel
    # We will initialize them with normal distributions and then inject anomalies.
    
    np.random.seed(42)
    
    # 1. Generate normal transactions (96% of data)
    amounts = np.random.exponential(scale=30.0, size=n_samples) + 2.00
    # Evict standard transactions exceeding $400
    amounts = np.where(amounts > 400.0, np.random.uniform(5.0, 150.0), amounts)
    
    tx_counts = np.random.poisson(lam=1.5, size=n_samples) + 1 # Poission centered at 1.5
    # tx_sum_10m is amount * multiplier
    tx_sums = amounts * np.random.uniform(1.0, 1.4, size=n_samples)
    
    impossible_travel = np.random.choice([0, 1], p=[0.995, 0.005], size=n_samples)
    
    labels = np.zeros(n_samples, dtype=int)
    
    # 2. Inject Fraud Profiles
    # Target 4% total fraud rate
    n_fraud = int(n_samples * 0.04)
    fraud_indices = np.random.choice(n_samples, size=n_fraud, replace=False)
    
    for idx in fraud_indices:
        fraud_type = np.random.choice(["amount_spike", "travel_anomaly", "burst_anomaly"])
        labels[idx] = 1
        
        if fraud_type == "amount_spike":
            # High amount spent in single transaction
            amounts[idx] = np.random.uniform(1200.0, 4500.0)
            tx_counts[idx] = np.random.randint(1, 3)
            tx_sums[idx] = amounts[idx] + np.random.uniform(0.0, 50.0)
            
        elif fraud_type == "travel_anomaly":
            # Physically impossible distance traveled
            impossible_travel[idx] = 1
            amounts[idx] = np.random.uniform(40.0, 400.0)
            tx_counts[idx] = np.random.randint(1, 4)
            tx_sums[idx] = amounts[idx] * np.random.uniform(1.0, 1.5)
            
        elif fraud_type == "burst_anomaly":
            # Rapid transaction storm
            tx_counts[idx] = np.random.randint(7, 18)
            amounts[idx] = np.random.uniform(50.0, 300.0)
            # High sum due to many small/medium transactions in short time
            tx_sums[idx] = np.random.uniform(600.0, 2500.0)

    # Compile DataFrame
    df = pd.DataFrame({
        "amount": amounts,
        "tx_count_10m": tx_counts,
        "tx_sum_10m": tx_sums,
        "impossible_travel": impossible_travel,
        "is_fraud": labels
    })
    
    # Ensure numerical sanity
    df["amount"] = df["amount"].round(2)
    df["tx_sum_10m"] = df["tx_sum_10m"].round(2)
    
    return df


def main():
    # 1. Synthesize Data
    df = synthesize_training_data()
    
    X = df[["amount", "tx_count_10m", "tx_sum_10m", "impossible_travel"]]
    y = df["is_fraud"]
    
    # 2. Train-Test Split (80% Train, 20% Test)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    
    print(f"Dataset summary: Train size: {len(X_train)}, Test size: {len(X_test)}")
    print(f"Fraud distribution in Train: {np.bincount(y_train)[1]} ({np.bincount(y_train)[1]/len(y_train)*100:.2f}%)")
    
    # 3. Model Training
    print("Training XGBoost Classifier...")
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        scale_pos_weight=15.0,  # Handle class imbalance (roughly 96:4 ratio)
        random_state=42,
        use_label_encoder=False,
        eval_metric="logloss"
    )
    
    model.fit(X_train, y_train)
    
    # 4. Evaluation
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    
    print("\n" + "="*40)
    print("Model Evaluation Metrics:")
    print("="*40)
    print(classification_report(y_test, y_pred))
    
    auc = roc_auc_score(y_test, y_proba)
    print(f"ROC-AUC Score: {auc:.4f}")
    
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))
    
    # 5. Export Model Artifact
    output_dir = os.path.dirname(__file__)
    model_path = os.path.join(output_dir, "model.bst")
    print(f"\nSaving model artifact to: {model_path}")
    model.save_model(model_path)
    print("Export complete.")

if __name__ == "__main__":
    main()
