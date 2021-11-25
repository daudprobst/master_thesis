import csv
import os
from src.graphs.line_plots import smoothed_line_trace
from src.analysis.timeseries import Timeseries

import plotly.graph_objects as go


def plot_ts_from_csv(file_name: str, output_folder: str, normalize=True, **kwargs):
    with open(file_name, "r") as f:
        reader = csv.reader(f)

        for row in reader:
            fig = go.Figure()
            firestorm_name = row[0]
            firestorm_ts = Timeseries(row[1:])
            day_breaks = [
                entry for entry in firestorm_ts.x if (entry % 24 == 0) and (entry != 0)
            ]
            for day_break in day_breaks:
                fig.add_vline(
                    x=day_break, line_width=1, line_dash="dot", line_color="grey"
                )
            if normalize:
                firestorm_ts.normalize()
            trace = smoothed_line_trace(
                firestorm_ts.y, firestorm_ts.x, name=firestorm_name, window_size=0
            )
            fig.add_trace(trace)
            fig.update_layout(**kwargs)
            fig.write_image(f"{output_folder}/{firestorm_name}.jpg")


if __name__ == "__main__":
    # Plotting quantity

    input_file = os.getcwd() + "/data/firestorm_quantities.csv"
    output_folder_name = os.getcwd() + "/plots/firestorm_overviews/quant_over_time"
    plot_ts_from_csv(
        input_file,
        output_folder_name,
        normalize=True,
        title="Tweet Quantities over Time",
        xaxis_title="Time",
        yaxis_title="Quantity of Tweets per Hour (normalized [0-1])",
    )

    # Plotting aggression
    aggr_file = os.getcwd() + "/data/firestorm_aggressions.csv"
    aggr_folder_name = os.getcwd() + "/plots/firestorm_overviews/aggr_over_time"
    plot_ts_from_csv(
        aggr_file,
        aggr_folder_name,
        normalize=True,
        title="Aggression Rate over Time",
        xaxis_title="Time",
        yaxis_title="Aggression Rate per Hour (normalized [0-1])",
    )
