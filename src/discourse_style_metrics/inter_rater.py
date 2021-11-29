import os
from collections import Counter
from typing import final
import pandas as pd
import numpy as np
from statsmodels.stats.inter_rater import fleiss_kappa, aggregate_raters


def inter_rater_agreement(file_path: str):
    labels_csv = pd.read_csv(file_path, sep="\t")

    labels_csv["label1"] = pd.factorize(labels_csv["label1"])[0]
    labels_csv["label2"] = pd.factorize(labels_csv["label2"])[0]
    labels_csv["label3"] = pd.factorize(labels_csv["label3"])[0]

    labels_pure = labels_csv[["label1", "label2", "label3"]].to_numpy()

    agg = aggregate_raters(labels_pure)
    return fleiss_kappa(agg[0], method="fleiss")


if __name__ == "__main__":
    file_path = os.getcwd() + "/data/aggr_sample/full_sample.csv"
    output_path = os.getcwd() + "/data/aggr_sample/aggregated_labels.csv"

    input_df = pd.read_csv(file_path, sep="\t")

    output_df = pd.DataFrame(columns=["text", "label"])
    output_df["text"] = input_df["text"]

    final_labels = []
    for name, row in input_df.iterrows():
        labels = [row["label1"], row["label2"], row["label3"]]
        label_aggregated = "OTHER"
        if labels.count("OFFENSE") > 1:
            label_aggregated = "OFFENSE"
        final_labels.append(label_aggregated)

    output_df["label"] = final_labels

    output_df.to_csv(output_path, sep="\t", index=False)
# print(agg)


# fleiss_kappa(table=labels_pure)
