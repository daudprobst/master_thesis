from src.utils.output_folders import PLOT_RAW_QUANTITIES_FOLDER, DATA_RAW_QUANTS_FOLDER
from src.ts_analysis.timeseries import load_ts_from_csv


if __name__ == "__main__":
    # Plotting quantity

    input_file = DATA_RAW_QUANTS_FOLDER + "raw_quantities.csv"

    quants_ts = load_ts_from_csv(input_file, only_trend=True, normalize=False)

    for ts_name, ts_obj in quants_ts:
        ts_obj.save_plot(
            output_folder=PLOT_RAW_QUANTITIES_FOLDER + "trends_smoothed/",
            name=ts_name,
            smoothing_window_size=23,
            xaxis_title="Time",
            yaxis_title="Tweets per Hour",
        )
        ts_obj.save_plot(
            output_folder=PLOT_RAW_QUANTITIES_FOLDER + "trends/",
            name=ts_name,
            smoothing_window_size=0,
            xaxis_title="Time",
            yaxis_title="Tweets per Hour",
        )
