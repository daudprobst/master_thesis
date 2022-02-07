import csv
from typing import List, Tuple

from statsmodels.tsa.seasonal import DecomposeResult, seasonal_decompose
import plotly.graph_objects as go
from src.graphs.line_plots import mark_day_breaks, smoothed_line_trace


class Timeseries:
    def __init__(self, y: list, x: list = None):
        self._y = [float(entry) for entry in y]
        if not x:
            self._x = list(range(len(y)))

    def __str__(self):
        return str(self._y)

    def __len__(self):
        return len(self._y)

    @property
    def x(self) -> list:
        return self._x

    @property
    def y(self) -> list:
        return self._y

    def normalize(self) -> None:
        max_val = max(self._y)
        self._y = [entry / max_val for entry in self._y]
        return self._y

    def decompose(self) -> DecomposeResult:
        return seasonal_decompose(self.y, period=24)

    def plot(
        self,
        smoothing_window_size: int = 0,
        mark_day_breaks_mode: str = None,
        **kwargs,
    ) -> go.Figure():
        fig = go.Figure()
        fig = mark_day_breaks(fig, self.x, mark_day_breaks_mode)
        trace = smoothed_line_trace(
            self.y,
            self.x,
            window_size=smoothing_window_size,
        )
        fig.add_trace(trace)
        fig.update_layout(**kwargs)
        return fig


def load_ts_from_csv(
    file_name, normalize: bool = False, only_trend: bool = False
) -> List[Tuple[str, Timeseries]]:
    data = []
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            firestorm_name = row[0]
            firestorm_ts = Timeseries(row[1:])
            if only_trend:
                try:
                    firestorm_ts = Timeseries(firestorm_ts.decompose().trend)
                except Exception as e:
                    print(
                        f"Extracting trend for {firestorm_name} was not possible. Skipping this one: {e}"
                    )
                    continue

            if normalize:
                firestorm_ts.normalize()

            data.append((firestorm_name, firestorm_ts))

    return data


if __name__ == "__main__":
    import csv

    from src.utils.output_folders import DATA_RAW_QUANTS_FOLDER

    timeseries_raw = load_ts_from_csv(
        DATA_RAW_QUANTS_FOLDER + "raw_quantities2.csv", normalize=True
    )
