from sklearn.model_selection import train_test_split
import numpy as np


def split_data(X_data, y_data, test_size=0.2, val_size=0.2):
    # Split data into train, val and test.
    # Note that 'shuffle=False' due to time-series data.
    X_train, X_test, y_train, y_test = train_test_split(X_data, y_data,
                                                        test_size=test_size,
                                                        shuffle=False)
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train,
                                                      test_size=val_size,
                                                      shuffle=False)

    # Convert from lists to Numpy arrays for reshaping purpose
    X_train = np.array(X_train)
    X_val = np.array(X_val)
    X_test = np.array(X_test)
    y_train = np.array(y_train)
    y_val = np.array(y_val)
    y_test = np.array(y_test)
    return X_train, X_val, X_test, y_train, y_val, y_test


def get_data_samples_kth_day(df, kth_day,
                             feature_slice,
                             window_size=30,
                             label_colID=3):
    # Split the dataset into time windows to get data samples.
    '''Predict label value of the next kth day based on multiple feature
    params:
        df: dataframe
        kth_day: predict for the kth day
        feature_slice: the slice of the features,
            i.e, slice(a,b) for multi-feature
            i.e, a for one feature, with a is the index of the label column
        window_size: the number of data points considered at a time
        label_colID: the column index of the label
       '''
    X_data = []
    y_data = []

    # Get the low, open, volume, high, close, adjustedclose
    for i in range(0, len(df) - window_size - kth_day):
        data_feature = []
        data_label = []

        # Get a window_size time frame for data feature
        for j in range(window_size):
            data_feature.append(
                df.iloc[i + j, feature_slice].to_numpy().tolist())

        # Next value is the label (price of the next day) to be predicted
        data_label.append(df.iloc[i + window_size + kth_day, label_colID])

        # Append new data sample (feature and label) to X_data and y_data
        X_data.append(np.array(data_feature).reshape(window_size, 6)) # TO DO: adjust these param
        y_data.append(np.array(data_label))
    return X_data, y_data


def get_data_samples_ks_day(df, k_days_ahead,
                            feature_slice,
                            window_size=30,
                            label_colID=3):
    # Split the dataset into time windows to get data samples.
    '''Predict label value of the next kth day based on multiple feature
    params:
        df: dataframe
        k_days_ahead: predict for the kth day
        feature_slice: the slice of the features,
            i.e, slice(a,b) for multi-feature
            i.e, a for one feature, with a is the index of the label column
        window_size: the number of data points considered at a time
        label_colID: the column index of the label
       '''
    X_data = []
    y_data = []

    # Get the low, open, volume, high, close, adjustedclose
    for i in range(0, len(df) - window_size - k_days_ahead):
        data_feature = []
        data_label = []

        # Get a window_size time frame for data feature
        for j in range(window_size):
            data_feature.append(
                df.iloc[i + j, feature_slice].to_numpy().tolist())

        # Next value is the label (price of the next day) to be predicted
        data_label.append(
            df.iloc[(i+window_size):(i+window_size+k_days_ahead), label_colID])

        # Append new data sample (feature and label) to X_data and y_data
        X_data.append(np.array(data_feature).reshape(window_size, 6))
        y_data.append(np.array(data_label).reshape(k_days_ahead,))
    return X_data, y_data
