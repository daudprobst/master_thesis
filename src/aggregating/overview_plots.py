import os

import plotly.graph_objects as go

from src.db.queried import QUERIES
from src.twitter_data.filters import equality_filter_factory
from src.db.connection import connect_to_mongo
from src.twitter_data.tweets import Tweets
from src.graphs.line_plots import smoothed_line_trace


def raw_quantity_plot(query_dicts: dict, output_folder: str):
    query_dicts = query_dicts.items()
    fig = go.Figure()
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Tweets per Hour",
    )
    for key, query_dict in query_dicts:
        print(f"Processing {key}")
        firestorm = Tweets.from_query(
            query_dict["query"], filters=[equality_filter_factory("lang", "de")]
        )

        single_fig = go.Figure()
        single_fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Tweets per Hour",
        )
        single_fig.add_trace(
            smoothed_line_trace(
                y=firestorm.hourwise_metrics["total_tweets"],
                x=firestorm.hourwise_metrics["hour"],
                name=key,
                window_size=0,
            )
        )
        single_fig.write_image(output_folder + f"{key}_raw.jpg")
        single_fig.add_hline(y=(max(firestorm.hourwise_metrics["total_tweets"]) * 0.2))
        single_fig.write_image(output_folder + f"{key}_raw_with_threshhold.jpg")
        fig.add_trace(
            smoothed_line_trace(
                y=firestorm.hourwise_metrics["total_tweets"],
                x=list(range(len(firestorm.hourwise_metrics["total_tweets"]))),
                name=key,
                window_size=0,
            )
        )
    fig.write_html(output_folder + "overview.html")


if __name__ == "__main__":
    connect_to_mongo()
    output_folder_name = os.getcwd() + "/plots/raw_data_quants/"
    raw_quantity_plot(QUERIES, output_folder_name)
