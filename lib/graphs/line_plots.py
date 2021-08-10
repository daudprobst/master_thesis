from typing import Sequence

import pandas as pd
from scipy import signal
import plotly.graph_objects as go


def smoothed_line_plots(ts_data: pd.DataFrame, x: str, y: Sequence[str], **kwargs) -> go:
    fig = go.Figure()
    print(kwargs)
    # window size used for filtering
    if 'window_size' in kwargs:
        window_size = kwargs.pop('window_size')
    else:
        window_size = 23

    # order of fitted polynomial
    if 'polynomial_order' in kwargs:
        polynomial_order = kwargs.pop('polynomial_order')
    else:
        polynomial_order = 3

    for y_entry in y:
        if window_size == 0:
            y_sig = ts_data[y_entry]
        else:
            y_sig = signal.savgol_filter(ts_data[y_entry], window_size, polynomial_order)

        fig.add_trace(go.Scatter(
            x=ts_data[x],
            y=y_sig,
            text=[f'Total tweets: {row["total_tweets"]}' for name, row in ts_data.iterrows()],
            name=y_entry
        ))

    fig.update_layout(**kwargs)
    return fig
