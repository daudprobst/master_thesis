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

    def decompose(self) -> DecomposeResult:
        return seasonal_decompose(self.y, period=24)

    def save_plot(
        self,
        output_folder: str,
        name: str,
        smoothing_window_size: int = 0,
        mark_day_breaks_mode: str = None,
        **kwargs,
    ) -> None:
        fig = go.Figure()
        fig = mark_day_breaks(fig, self.x, mark_day_breaks_mode)
        trace = smoothed_line_trace(
            self.y,
            self.x,
            name=name,
            window_size=smoothing_window_size,
        )
        fig.add_trace(trace)
        fig.update_layout(**kwargs)

        fig.write_image(f"{output_folder}{name}.jpg")


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

    with open(DATA_RAW_QUANTS_FOLDER + "raw_quantities.csv", "r") as f:
        reader = csv.reader(f)

        for row in reader:
            ts = Timeseries(row[1:])
            plt = ts.decompose().plot()
            print(type(plt))
            plt.show()
            break  # TODO remove this limitation to just the first ts
