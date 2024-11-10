import unittest
import pandas as pd
import numpy as np
from src.etl.preprocess import get_data_samples_ks_day, split_data
from src.etl.preprocess import get_data_samples_kth_day, get_num_features
# python -m unittest discover -s test/etl -p "test_preprocess.py"


class TestPreprocess(unittest.TestCase):
    def setUp(self):
        # Sample DataFrame for testing
        data = {
            'low': np.arange(1, 11),
            'open': np.arange(2, 12),
            'volume': np.arange(10, 110, 10),
            'high': np.arange(3, 13),
            'close': np.arange(2.5, 12.5, 1),
            'adjustedclose': np.arange(2.4, 12.4, 1),
            'label': np.arange(3, 13)  # This is the label column
        }
        self.df = pd.DataFrame(data)

    def test_get_data_samples_kth_day(self):
        X, y = get_data_samples_kth_day(
            self.df, kth_day=1, feature_slice=slice(0, 6), window_size=3)
        self.assertEqual(len(X), 6)  # There should be 6 samples
        # Each sample should be of shape (window_size, num_features)
        self.assertEqual(X[0].shape, (3, 6))
        self.assertEqual(y[0], 6)  # Check the first label

    def test_get_data_samples_ks_day(self):
        X, y = get_data_samples_ks_day(
            self.df, k_days_ahead=2, feature_slice=slice(0, 6), window_size=3)
        self.assertEqual(len(X), 5)  # There should be 5 samples
        # Each sample should be of shape (window_size, num_features)
        self.assertEqual(X[0].shape, (3, 6))
        # Check the first label (two days ahead)
        self.assertTrue(np.array_equal(y[0], np.array([6, 7])))

    def test_split_data(self):
        X_data = [i for i in range(10)]
        y_data = [i for i in range(10)]
        X_train, X_val, X_test, y_train, y_val, y_test = split_data(
            X_data, y_data, test_size=0.2, val_size=0.2)

        self.assertEqual(len(X_train), 6)  # 60% train
        self.assertEqual(len(X_val), 2)    # 20% validation
        self.assertEqual(len(X_test), 2)   # 20% test

    def test_get_num_features(self):
        self.assertEqual(get_num_features(
            slice(0, 3), self.df), 3)  # Expect 3 features
        self.assertEqual(get_num_features(
            slice(2, None), self.df), 5)  # Expect 5 features
        self.assertEqual(get_num_features(
            slice(None, 4), self.df), 4)  # Expect 4 features
        self.assertEqual(get_num_features(slice(None, None),
                         self.df), 7)  # Expect all features


if __name__ == "__main__":
    unittest.main()
