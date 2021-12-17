from pandas.plotting import autocorrelation_plot
import matplotlib.pyplot as plt
import os
import csv
from ts_analysis.timeseries import Timeseries


def plot_autocorrelation(file_name: str, output_folder: str, normalize=True, **kwargs):
    with open(file_name, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            firestorm_name = row[0]
            print(firestorm_name)
            firestorm_ts = Timeseries(row[1:])
            autocorrelation_plot(firestorm_ts.y)
            plt.acorr(firestorm_ts.y, maxlags=10)
            plt.show()
            break


if __name__ == "__main__":
    # Plotting quantity
    input_file = os.getcwd() + "/data/firestorm_quantities.csv"
    output_folder_name = os.getcwd() + "/plots/firestorm_overviews/quant_over_time"
    plot_autocorrelation(
        input_file,
        output_folder_name,
    )
