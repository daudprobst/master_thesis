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
