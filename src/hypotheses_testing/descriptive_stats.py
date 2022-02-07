import pandas as pd
from src.db.connection import connect_to_mongo
from src.db.thesis_queries import load_hypothesis_dataset
from src.graphs.line_plots import smoothed_line_trace
import plotly.express as px

import plotly.graph_objects as go
from src.utils.output_folders import PLOT_BASE_FOLDER
from src.ts_analysis.timeseries import Timeseries, load_ts_from_csv
import seaborn as sns


def user_aggr_plot():
    tweets_df = load_hypothesis_dataset(["is_offensive", "user_activity"])
    bins = list(range(0, tweets_df["user_activity"].max() + 10, 10))
    tweets_df["user_activity_bins"] = pd.cut(tweets_df["user_activity"], bins=bins)
    tweets_df.drop("user_activity", axis=1, inplace=True)
    count_per_bin = tweets_df.groupby("user_activity_bins").count()
    low_count_bins = count_per_bin[lambda x: x < 1000].dropna().index
    filter_stats = (
        ~tweets_df["user_activity_bins"].isin(low_count_bins)
    ).value_counts()
    print(
        f"Due to avoiding low quantity bins, {filter_stats[False]} of {filter_stats[False] + filter_stats[True]} tweets are removed."
    )
    tweets_df_filtered = tweets_df[
        ~tweets_df["user_activity_bins"].isin(low_count_bins)
    ]


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


def _group_consecutive_numbers(input: list[int]):
    consecutives = []
    new_list = None
    for entry in input:
        if not new_list:
            new_list = [entry]
        elif new_list[-1] == entry - 1:
            new_list.append(entry)
        else:
            consecutives.append(new_list)
            new_list = [entry]
    consecutives.append(new_list)
    return consecutives


def corr_heatmap(tweets_dummified: pd.DataFrame):
    correlation_map = tweets_dummified.corr(method="pearson")
    cleaner_var_names = [
        "12am-06am",
        "06am-12pm",
        "12pm-06pm",
        "pre-hype",
        "post-hype",
        "original tweet",
        "reply",
        "quoted",
        "active user",
        "hyperactive user",
        "is_aggressive",
    ]
    fig = px.imshow(
        correlation_map,
        x=cleaner_var_names,
        y=cleaner_var_names,
        labels={"color": "Pearson's correlation"},
    )
    fig.update_layout(width=700, height=500)
    return fig


def absolute_thresh_pruning_plot(ts: Timeseries, **kwargs) -> go.Figure:
    fig = ts.plot()
    fig.add_hline(y=0.2, line_dash="dash", line_width=2)
    above_thresh = [x for (x, y) in zip(ts.x, ts.y) if y >= 0.2]
    consecutives = _group_consecutive_numbers(above_thresh)
    print(consecutives)
    for consecutive in consecutives:
        fig.add_vrect(
            x0=min(consecutive),
            x1=max(consecutive),
            fillcolor="LightSalmon",
            opacity=0.5,
            layer="below",
            line_width=0,
        ),
    fig.update_layout(**kwargs)
    return fig


if __name__ == "__main__":

    timeseries_raw = load_ts_from_csv(
        "/home/david/Desktop/Masterarbeit/twit_scrape/data/raw_quantities/raw_quantities2.csv",
        normalize=True,
    )

    for name, ts in timeseries_raw:
        if name == "amthor":
            amthor_ts = ts

    absolute_thresh_pruning_plot(amthor_ts)
