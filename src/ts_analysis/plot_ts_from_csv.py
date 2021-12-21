from src.utils.output_folders import (
    PLOT_TS_AGGRESSION_FOLDER,
    DATA_TIME_SERIES_FOLDER,
    PLOT_TS_MIXED_FOLDER,
)

from typing import List, Tuple

import plotly.graph_objects as go
from src.graphs.line_plots import smoothed_line_trace, mark_day_breaks
from src.ts_analysis.timeseries import Timeseries, load_ts_from_csv


def plot_multivariate_ts(
    ts_objs: List[Tuple[str, Timeseries]],
    output_filepath: str,
    day_breaks_mark_mode: str = None,
    smoothing_window_size: int = 0,
    **kwargs,
):
    fig = go.Figure()
    fig = mark_day_breaks(fig, ts_objs[0][1].x, day_breaks_mark_mode)

    for dim_name, dim_ts in ts_objs:
        trace = smoothed_line_trace(
            dim_ts.y,
            dim_ts.x,
            name=dim_name,
            window_size=smoothing_window_size,
        )
        fig.add_trace(trace)
    fig.update_layout(**kwargs)

    fig.write_image(f"{output_filepath}.jpg")


def plot_multiple_ts(
    data: list[Timeseries],
    entry_names: list[str],
    measure_names: list[str],
    output_folder: str,
    day_breaks_mark_mode: str = None,
    smoothing_window_size: int = 0,
    **kwargs,
):

    zippedFirestormsData = list(zip(*data))

    for zippedFirestorm in zippedFirestormsData:
        fig = go.Figure()
        fig = mark_day_breaks(fig, zippedFirestorm[0].x, day_breaks_mark_mode)
        for i, ts_entry in enumerate(zippedFirestorm):
            trace = smoothed_line_trace(
                ts_entry.y,
                ts_entry.x,
                name=measure_names[i],
                window_size=smoothing_window_size,
            )
            fig.add_trace(trace)
        fig.update_layout(**kwargs)

        fig.write_image(f"{output_folder}{entry_names[i]}.jpg")


if __name__ == "__main__":

    aggression_ts_list = load_ts_from_csv(
        DATA_TIME_SERIES_FOLDER + "aggression_ts.csv", normalize=False, only_trend=False
    )

    quants_ts_list = load_ts_from_csv(
        DATA_TIME_SERIES_FOLDER + "quantities_ts.csv", normalize=True, only_trend=False
    )

    for ts_name, ts_obj in aggression_ts_list:
        ts_obj.save_plot(
            output_folder=PLOT_TS_AGGRESSION_FOLDER,
            name=ts_name,
            smoothing_window_size=5,
            xaxis_title="Time",
            yaxis_title="Tweets per Hour",
        )

    for i in range(len(quants_ts_list)):
        ts_name = aggression_ts_list[i][0]
        plot_multivariate_ts(
            ts_objs=[
                ("aggression", aggression_ts_list[i][1]),
                ("quantity", quants_ts_list[i][1]),
            ],
            output_filepath=PLOT_TS_MIXED_FOLDER + ts_name,
            day_breaks_mark_mode="lines",
            smoothing_window_size=5,
            xaxis_title="Time",
            # **kwargs,
        )
    """
    plot_ts_from_csv(
        file_name=DATA_TIME_SERIES_FOLDER + "aggression_ts.csv",
        output_folder=PLOT_TS_AGGRESSION_FOLDER,
        normalize=False,
        day_breaks_mark_mode="lines",
        smoothing_window_size=5,
    )

    multiplot_ts_from_csvs(
        file_names=[
            DATA_TIME_SERIES_FOLDER + "aggression_ts.csv",
            DATA_TIME_SERIES_FOLDER + "quantities_ts.csv",
        ],
        output_folder=PLOT_TS_MIXED_FOLDER,
        normalize=False,
        only_trend=False,
        day_breaks_mark_mode="lines",
        smoothing_window_size=5,
    )
    """
