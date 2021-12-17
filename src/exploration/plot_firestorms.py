from src.utils.output_folders import PLOT_RAW_QUANTITIES_FOLDER, DATA_RAW_QUANTS_FOLDER
from src.ts_analysis.plot_ts_from_csv import plot_ts_from_csv


if __name__ == "__main__":
    # Plotting quantity

    input_file = DATA_RAW_QUANTS_FOLDER + "raw_quantities.csv"
    output_folder_name = PLOT_RAW_QUANTITIES_FOLDER + "trends_smoothed/"
    plot_ts_from_csv(
        input_file,
        output_folder_name,
        only_trend=True,
        mark_day_breaks="ticks",
        smoothing_window_size=23,
        title=None,
        xaxis_title=None,
        yaxis_title="Moving Average of Tweets per Hour",
    )

    # Plotting aggression
    """
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
    """


"""
firestorm_quantity_trend_settings = 
            fig.update_layout(
                paper_bgcolor="white",
                plot_bgcolor="white",
                yaxis_showgrid=True,
                yaxis_gridcolor="black",
                yaxis_zeroline=True,
                yaxis_zerolinecolor="black",
                xaxis_zeroline=True,
                xaxis_zerolinecolor="black",
                xaxis_showgrid=False
                
            )
"""
