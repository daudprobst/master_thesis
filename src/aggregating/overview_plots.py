import os

import plotly.graph_objects as go

from src.db.queried import QUERIES
from src.twitter_data.filters import equality_filter_factory
from src.db.connection import connect_to_mongo
from src.twitter_data.tweets import Tweets
from src.graphs.line_plots import smoothed_line_trace
from src.aggregating.pruning_raw_firestorm import (
    get_threshold,
    get_firestorm_wrapping_datetime,
)


def raw_quantity_plot(query_dicts: dict, output_folder: str):
    query_dicts = query_dicts.items()

    for key, query_dict in query_dicts:
        print(f"Processing {key}")
        firestorm = Tweets.from_query(
            query_dict["query"], filters=[equality_filter_factory("lang", "de")]
        )

        layout = go.Layout(
            xaxis={
                "title": "Date",
                "showgrid": False,
                "showline": False,
            },
            yaxis={
                "title": "Tweets per Hour",
                "showgrid": False
            }
        )

        fig = go.Figure(layout=layout)

        # add daily separation line
        """
        day_breaks = [entry for entry in firestorm.hourwise_metrics.index if entry.hour == 0]
        for day_break in day_breaks:
            fig.add_vline(
                x=day_break, line_width=1, line_dash="dot", line_color="grey"
            )
        """

        # add quantity curve itself
        fig.add_trace(
            smoothed_line_trace(
                y=firestorm.hourwise_metrics["total_tweets"],
                x=firestorm.hourwise_metrics["hour"],
                name=key,
                window_size=0,
            )
        )

        # write raw images
        fig.write_image(output_folder + f"{key}_raw.jpg")


        # add threshold line
        fig.add_hline(y=get_threshold(firestorm), line_dash="dash", line_width=2)
        
        # add selection

        start_date, end_date = get_firestorm_wrapping_datetime(firestorm)
        if start_date:
            fig.add_vrect(
                x0=start_date,
                x1=end_date,
                fillcolor="LightSalmon",
                opacity=0.5,
                layer="below",
                line_width=0,
            ),
        fig.write_image(output_folder + f"{key}_pruned.jpg")


if __name__ == "__main__":
    connect_to_mongo()
    output_folder_name = os.getcwd() + "/plots/pruning/"
    raw_quantity_plot(QUERIES, output_folder_name)
