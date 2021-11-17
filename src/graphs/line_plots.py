from typing import Sequence

import pandas as pd
from scipy import signal
import plotly.graph_objects as go


def df_smoothed_line_plots(
    ts_data: pd.DataFrame, x_attr: str, y_attrs: Sequence[str], **kwargs
) -> go.Figure:
    fig = go.Figure()

    for y_attr in y_attrs:
        fig.add_trace(
            smoothed_line_trace(
                x=ts_data[x_attr],
                y=ts_data[y_attr],
                name=y_attr,
            )
        )

    fig.update_layout(**kwargs)
    return fig


def smoothed_line_trace(
    y: list,
    x: list,
    name: str = "unnamed",
    window_size: int = 23,
    polynomial_order: int = 3,
) -> go.Scatter():

    if window_size != 0:  # no smoothing if window size 0 is passed
        y = signal.savgol_filter(y, window_size, polynomial_order)

    return go.Scatter(x=x, y=y, name=name)
