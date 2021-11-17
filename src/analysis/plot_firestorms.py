import csv
import os
from src.graphs.line_plots import smoothed_line_trace, df_smoothed_line_plots
from src.analysis.timeseries import Timeseries

import plotly.graph_objects as go


def quantity_plot(file_name: str, normalize=False):
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        fig = go.Figure()

        for row in reader:
            firestorm_name = row[0]
            firestorm_data = row[1:]
            firestorm_ts = Timeseries(firestorm_data)
            if normalize:
                firestorm_ts.normalize()
            trace = smoothed_line_trace(
                firestorm_ts.y, firestorm_ts.x, name=firestorm_name, window_size=0
            )
            fig.add_trace(trace)
    fig.show()


if __name__ == "__main__":
    quantity_plot(os.getcwd() + "/data/firestorm_quantities.csv", normalize=True)
