import os
import csv
from statistics import mean


file_name = os.getcwd() + "/data/firestorm_overview.csv"


# Calculate cuts due to filtering
with open(file_name, "r") as f:
    reader = csv.DictReader(f)

    cuts = []
    cuts2 = []
    for row in reader:
        firestorm_name = row["key"]
        filter_log = (
            row["filtering_lengths_log"].replace("[", "").replace("]", "").split(",")
        )
        filter_log = [int(entry) for entry in filter_log]
        print(firestorm_name)
        print(filter_log)
        cut = 1 - (filter_log[1] / filter_log[0])
        cuts.append(cut)
        print(cut)

        cut2 = 1 - (filter_log[2] / filter_log[1])
        cuts2.append(cut2)

    print("===================")
    print(mean(cuts))
    print(mean(cuts2))
