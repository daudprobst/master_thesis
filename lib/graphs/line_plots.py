from typing import Sequence

import pandas as pd
from scipy import signal
import plotly.graph_objects as go

def smoothed_line_plots(ts_data: pd.DataFrame, x: str, y: Sequence[str]) -> go:
    fig = go.Figure()
    for y_entry in y:
        fig.add_trace(go.Scatter(
            x= ts_data[x],
            y= signal.savgol_filter(ts_data[y_entry],
                                   27,  # window size used for filtering
                                   3),  # order of fitted polynomial
        name=y_entry
        ))
    return fig