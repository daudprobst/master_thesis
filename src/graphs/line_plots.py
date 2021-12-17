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


def mark_day_breaks(fig, x, mode=None):
    if not mode:
        return fig

    day_breaks = [entry for entry in x if (entry % 24 == 0) and (entry != 0)]

    if mode == "lines":
        for day_break in day_breaks:
            fig.add_vline(
                x=day_break,
                line_width=1,
                line_dash="dot",
                line_color="grey",
            )

    elif mode == "ticks":
        # Add daily tickmarks
        day_centers = [day_break - 12 for day_break in day_breaks]

        fig.update_layout(
            xaxis=dict(
                tickmode="array",
                tickvals=day_centers,
                ticktext=[f"Day {index +1}" for index, _ in enumerate(day_centers)],
            )
        )

    return fig
