import numpy as np
from sklearn.ensemble import RandomForestRegressor

from galileojp.munger import to_metric_table_series


def train_rf_power_estimator(telem, node: str, window='1s', metrics=None) -> RandomForestRegressor:
    tdf = to_metric_table_series(telem, node, window=window, metrics=metrics)

    df = tdf.dropna()
    if not len(df):
        raise ValueError("Empty data frame")

    features = list(df.columns.values)
    features.pop(features.index('watt'))

    X = np.asarray(df[features])
    y = np.asarray(df['watt'])

    clf = RandomForestRegressor(n_estimators=100, max_depth=50, random_state=0)
    clf.fit(X, y)

    return clf
