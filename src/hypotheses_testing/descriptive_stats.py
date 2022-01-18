import pandas as pd
from src.db.connection import connect_to_mongo
from src.hypotheses_testing.helpers import (
    load_hypothesis_dataset,
)
from src.graphs.line_plots import smoothed_line_trace

import plotly.graph_objects as go
from src.utils.output_folders import PLOT_BASE_FOLDER


def user_activity_plot():
    tweets_df = load_hypothesis_dataset(["is_offensive", "user_activity"])
    bins = list(range(0, tweets_df["user_activity"].max() + 10, 10))
    tweets_df["user_activity_bins"] = pd.cut(tweets_df["user_activity"], bins=bins)
    tweets_df.drop("user_activity", axis=1, inplace=True)
    count_per_bin = tweets_df.groupby("user_activity_bins").count()
    low_count_bins = count_per_bin[lambda x: x < 3000].dropna().index
    filter_stats = (
        ~tweets_df["user_activity_bins"].isin(low_count_bins)
    ).value_counts()
    print(
        f"Due to avoiding low quantity bins, {filter_stats[False]} of {filter_stats[False] + filter_stats[True]} tweets are removed."
    )
    tweets_df_filtered = tweets_df[
        ~tweets_df["user_activity_bins"].isin(low_count_bins)
    ]
    bin_average_aggression = (
        tweets_df_filtered.groupby("user_activity_bins").mean().is_offensive.dropna()
    )
    line_trace = smoothed_line_trace(
        y=bin_average_aggression.values,
        x=[str(entry) for entry in bin_average_aggression.index],
        window_size=0,
    )
    fig = go.Figure()
    fig.update_layout(width=700, height=500)
    fig.update_layout(
        yaxis_title="Percentage of Aggressive Tweets",
        xaxis_title="Total Tweets by Author",
    )
    fig.add_trace(line_trace)
    fig.write_image(PLOT_BASE_FOLDER + "aggresion_by_author_activity_bin.pdf")


def user_type_mean_tweets():
    full_df = load_hypothesis_dataset(["user_type", "author_id"])
    for name, df in full_df.groupby("user_type"):
        print(
            f'There are {len(df)} tweet from user_group {name} from {len(df["author_id"].unique())} users'
        )
        print(df.groupby("author_id").count()["_id"].describe())
        print("=================")
