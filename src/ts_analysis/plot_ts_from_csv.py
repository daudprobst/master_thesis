from src.utils.output_folders import (
    PLOT_TS_AGGRESSION_FOLDER,
    DATA_TIME_SERIES_FOLDER,
    PLOT_TS_MIXED_FOLDER,
)
import csv
import os
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


def multiplot_ts_from_csvs(
    file_names: list[str],
    output_folder: str,
    normalize: bool = True,
    only_trend: bool = False,
    day_breaks_mark_mode: str = None,
    smoothing_window_size: int = 0,
    **kwargs,
):
    """[summary]

    :param file_name: Names of files which should be combined in plot; The rows must be in the same order in
    each csv (there should be the same number of rows in each csv)
    :param output_folder: Path to folder in which the output plots will be saved
    :param normalize: Whether the values in each timeseries should be normalized between 0 and 1, defaults to True
    :param only_trend: If true, only the trend is shown (overwrites normalize), defaults to False
    :param day_breaks_mark_mode: [description], defaults to None
    :param smoothing_window_size: [description], defaults to 0
    """
    timeseries_by_csvs = []

    measure_names = [
        os.path.basename(file_path).split(".")[0] for file_path in file_names
    ]
    # Write out the headers
    with open(file_names[0], "r") as f:
        reader = csv.reader(f)
        names = []
        for row in reader:
            names.append(row[0])
        timeseries_by_csvs.append(names)

    # Write out actual data
    for i, file_name in enumerate(file_names):
        timeseries_for_csv = []
        with open(file_name, "r") as f:
            reader = csv.reader(f)
            output_data = []
            for row in reader:
                firestorm_name = row[0]
                firestorm_ts = Timeseries(row[1:])
                if normalize:
                    firestorm_ts.normalize()

                output_data.append((firestorm_name, firestorm_ts))

                timeseries_for_csv.append(firestorm_ts)
        timeseries_by_csvs.append(timeseries_for_csv)

    zippedFirestormsData = list(zip(*timeseries_by_csvs))

    for zippedFirestorm in zippedFirestormsData:
        fig = go.Figure()
        firestorm_name = zippedFirestorm[0]
        firestorm_data = zippedFirestorm[1:]
        fig = mark_day_breaks(fig, firestorm_data[0].x, day_breaks_mark_mode)
        for i, ts_entry in enumerate(firestorm_data):
            if only_trend:
                try:
                    trend = ts_entry.decompose().trend
                    trace = smoothed_line_trace(
                        trend,
                        ts_entry.x,
                        name=measure_names[i],
                        window_size=smoothing_window_size,
                    )
                except Exception as e:
                    print(
                        f"Extracting trend for {firestorm_name} was not possible. Skipping this one: {e}"
                    )
                    continue
            else:
                trace = smoothed_line_trace(
                    ts_entry.y,
                    ts_entry.x,
                    name=measure_names[i],
                    window_size=smoothing_window_size,
                )
            fig.add_trace(trace)
        fig.update_layout(**kwargs)

        fig.write_image(f"{output_folder}{firestorm_name}.jpg")


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
