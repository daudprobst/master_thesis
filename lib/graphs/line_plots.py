from typing import Sequence

import pandas as pd
from scipy import signal
import plotly.graph_objects as go


def smoothed_line_plots(ts_data: pd.DataFrame, x: str, y: Sequence[str], **kwargs) -> go:
    fig = go.Figure()

    # window size used for filtering
    if 'window_size' in kwargs:
        window_size = kwargs['window_size']
    else:
        window_size = 23

    # order of fitted polynomial
    if 'polynomial_order' in kwargs:
        polynomial_order = kwargs['polynomial_order']
    else:
        polynomial_order = 3
    for y_entry in y:
        fig.add_trace(go.Scatter(
            x=ts_data[x],
            y=signal.savgol_filter(ts_data[y_entry], window_size, polynomial_order),
            name=y_entry
        ))
    return fig
