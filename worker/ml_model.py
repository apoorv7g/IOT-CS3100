# distributed/worker/ml_model.py
import numpy as np
from sklearn.ensemble import IsolationForest
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")


class MLModel:
    def __init__(self):
        # Train a demo IsolationForest on 5-D synthetic data
        X = np.random.rand(400, 5) * 100.0
        self.model = IsolationForest(contamination=0.1, random_state=42)
        self.model.fit(X)
        logging.info("ML model initialized and trained on synthetic data")

    def predict(self, batch_data):
        """
        batch_data: dict with keys ["temperature","humidity","pressure","light","vibration"]
        Returns: list of 5 prediction values (1 or -1). For simplicity, we apply model to single sample.
        """
        try:
            features = ["temperature", "humidity", "pressure", "light", "vibration"]
            X = np.array([[float(batch_data.get(f, 0)) for f in features]])
        except Exception:
            X = np.zeros((1, 5))
        preds = self.model.predict(X)  # shape (1,)
        # Expand to length-5 for charting convenience (same pred for all features)
        val = int(preds[0])
        return [val, val, val, val, val]
