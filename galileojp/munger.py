import numpy as np
import pandas as pd


def to_metric_table_series(telem, node, window='1s', metrics=None):
    """
    Transforms the long-format telemetry timeseries into a wide-timeseries by resampling in a window and creating
    columns for each metric.

    :param telem: the telemetry data frame returned by ExperimentFrameGateway.telemetry()
    :param node: the node to filter
    :param window: the window size for resampling (default = 1s)
    :param metrics: a list of metrics to filter
    :return: a new dataframe
    """
    df = telem[telem['NODE'] == node][['METRIC', 'VALUE']]
    metrics = metrics or list(df.METRIC.unique())

    data = dict()

    for metric in metrics:
        time_series = df[df['METRIC'] == metric]
        time_series = pd.Series(data=np.asarray(time_series['VALUE']), index=time_series.index)
        time_series.resample(window).mean()
        data[metric] = time_series

    return pd.DataFrame(data, index=data[metrics[0]].index)
