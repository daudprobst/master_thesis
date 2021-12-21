import os

# WD
WORKING_DIRECTORY = os.getcwd()

# Data folders

DATA_BASE_FOLDER = WORKING_DIRECTORY + "/data/"
DATA_RAW_QUANTS_FOLDER = DATA_BASE_FOLDER + "raw_quantities/"
DATA_TIME_SERIES_FOLDER = DATA_BASE_FOLDER + "pruned_timeseries/"
DATA_FIRESTORMS_FOLDER = DATA_BASE_FOLDER + "firestorms/"

# Plot Folders
PLOT_BASE_FOLDER = WORKING_DIRECTORY + "/plots/"
PLOT_RAW_QUANTITIES_FOLDER = PLOT_BASE_FOLDER + "raw_quantities/"
PLOT_TS_PRUNING_FOLDER = PLOT_BASE_FOLDER + "pruned_timeseries/pruning/"
PLOT_TS_AGGRESSION_FOLDER = PLOT_BASE_FOLDER + "pruned_timeseries/aggression/"
PLOT_TS_MIXED_FOLDER = PLOT_BASE_FOLDER + "pruned_timeseries/mixed/"
