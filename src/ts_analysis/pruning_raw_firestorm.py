from datetime import datetime, timedelta
from typing import Tuple

import plotly.graph_objects as go
from src.db.connection import connect_to_mongo
from src.db.queried import QUERIES, query_iterator
from src.graphs.line_plots import smoothed_line_trace
from src.twitter_data.filters import de_filter
from src.twitter_data.tweets import Tweets
from src.utils.output_folders import PLOT_TS_PRUNING_FOLDER


MIN_THRESHOLD = 100
THRESHOLD_FACTOR = 0.2


def get_threshold(tweets: Tweets) -> int:
    """Calculates the threshold of tweets after which a Firestorm is considered to start/end

    :param tweets: collection of tweets for which the shitstorm should be calculated
    :return: number of tweets that are the threshold
    """
    return max(
        MIN_THRESHOLD, (THRESHOLD_FACTOR * max(tweets.hourwise_metrics["total_tweets"]))
    )


def get_firestorm_wrapping_datetime(tweets: Tweets) -> Tuple[datetime, datetime]:
    """Calculates the start and end datetimes for a Firestorm by checking when the Firestorm
        crosses the defined threshold

    :param tweets: Tweets for which the start and end should be determined
    :return: Tuple of 1) start_date (inclusive), end_date(exclusive)
    """
    threshold = get_threshold(tweets)
    tweets_grouped_by_day = tweets.hourwise_metrics.groupby(lambda x: x.date)

    start_date = None
    end_date = None
    for group_name, df_group in tweets_grouped_by_day:
        max_tweets_on_day = max(df_group["total_tweets"])
        if not start_date:
            if max_tweets_on_day >= threshold:
                start_date = group_name
        else:
            if (
                max_tweets_on_day < threshold
            ):  # first day after start day that drops below threshold (should be excluded)
                end_date = group_name - timedelta(days=1)
                break

    if not start_date:
        return None, None  # Firestorm never goes above the threshold

    start_datetime = None
    for name, row in tweets_grouped_by_day.get_group(start_date).iterrows():
        if row["total_tweets"] >= threshold:
            start_datetime = name
            break  # break after first datetime on day is found

    end_datetime = None
    for name, row in tweets_grouped_by_day.get_group(end_date).iterrows():
        if row["total_tweets"] >= threshold:
            end_datetime = name + timedelta(
                hours=1
            )  # last datetime of  the date overwrites this entry
            # we add one hour as the end_datetime should be exclusive

    return start_datetime, end_datetime


def pruning_plot(query_dicts: dict, output_folder: str) -> None:
    """Generates a plot that visualizes the pruning process

    :param query_dicts: Dict of query_dicts for which the pruning process should be visualized
    :param output_folder: visualization will be saved to this folder
    """

    for key, query_dict in query_iterator(
        query_dicts=query_dicts, include_timeseries_disabled=False
    ):
        firestorm = Tweets.from_query(query_dict["query"], filters=[de_filter()])

        layout = go.Layout(
            xaxis={
                "title": "Date",
                "showgrid": False,
                "showline": False,
            },
            yaxis={"title": "Tweets per Hour", "showgrid": False},
        )

        fig = go.Figure(layout=layout)

        # add quantity curve itself
        fig.add_trace(
            smoothed_line_trace(
                y=firestorm.hourwise_metrics["total_tweets"],
                x=firestorm.hourwise_metrics["hour"],
                name=key,
                window_size=0,
            )
        )

        # add threshold line
        fig.add_hline(y=get_threshold(firestorm), line_dash="dash", line_width=2)

        # add selection

        start_date, end_date = get_firestorm_wrapping_datetime(firestorm)
        print(f"{key} has start_date {start_date} and end_date {end_date}")

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
    pruning_plot(QUERIES, PLOT_TS_PRUNING_FOLDER)
