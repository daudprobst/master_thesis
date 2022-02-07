import pandas as pd
import numpy as np
from src.ts_analysis.timeseries import Timeseries, load_ts_from_csv
from src.utils.output_folders import DATA_HYPE_PHASE_TS_FOLDER
import scipy.stats as stats


def ts_pearson_correlation(A: Timeseries, B: Timeseries, lag: int = 0):
    if len(A) != len(B):
        raise ValueError(
            "The two time series must be of the same length to test for correlation."
        )
    ys_as_df = pd.DataFrame(list(zip(A.y, B.y)))

    if lag >= 0:
        # drop from end
        seriesA = ys_as_df.iloc[:, 0].drop(ys_as_df.tail(lag).index)
        # drop from start (shifted ts)
        seriesB = ys_as_df.iloc[:, 1].drop(ys_as_df.head(lag).index)
    else:
        # negative shift is equal to shifting the other ts
        # drop from start (shifted ts)
        seriesA = ys_as_df.iloc[:, 0].drop(ys_as_df.head(-lag).index)
        # drop from end
        seriesB = ys_as_df.iloc[:, 1].drop(ys_as_df.tail(-lag).index)

    return stats.pearsonr(seriesA, seriesB)


def significance_mark(p: float) -> str:
    if p < 0.001:
        return "***"
    elif p < 0.01:
        return "**"
    elif p < 0.05:
        return "*"

    return ""


if __name__ == "__main__":
    aggression_ts_list = load_ts_from_csv(
        DATA_HYPE_PHASE_TS_FOLDER + "aggression_ts.csv", normalize=True
    )

    quants_ts_list = load_ts_from_csv(
        DATA_HYPE_PHASE_TS_FOLDER + "quantities_ts.csv", normalize=True
    )

    zipped_ts = list(zip(aggression_ts_list, quants_ts_list))

    for (firestorm_name, aggresion_ts), (firestorm_name2, quants_ts) in zipped_ts:
        if firestorm_name != firestorm_name2:
            raise ValueError(
                "Firestorm names not identical: {firestorm_name} and {firestorm_name2}"
            )
        print("==========")
        print(firestorm_name)

        # Pearson Corr with lags
        lagged_corrs = [
            ts_pearson_correlation(aggresion_ts, quants_ts, lag) for lag in range(-3, 4)
        ]
        for i, (r, p) in enumerate(lagged_corrs, start=-3):
            print(
                f'{i}: p: {np.round(p,2)}, r: {np.round(r,4)} [{("POS" if p >=0 else "NEG")}]{significance_mark(p)}'
            )
