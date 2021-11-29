from datetime import datetime, timedelta
from typing import Tuple

import plotly.graph_objects as go

from src.db.queried import QUERIES
from src.twitter_data.filters import equality_filter_factory
from src.db.connection import connect_to_mongo
from src.twitter_data.tweets import Tweets
from src.graphs.line_plots import smoothed_line_trace


MIN_THRESHOLD = 100
THRESHOLD_FACTOR = 0.2


def get_threshold(tweets: Tweets) -> int:
    return max(
        MIN_THRESHOLD, (THRESHOLD_FACTOR * max(tweets.hourwise_metrics["total_tweets"]))
    )


def get_firestorm_wrapping_datetime(tweets: Tweets) -> Tuple[datetime, datetime]:
    """Calculates the start and end datetimes for a Firestorm by checking when the Firestorm
        crosses the defined threshold

    :param tweets: [description]
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


if __name__ == "__main__":
    connect_to_mongo()

    for key, query_dict in QUERIES.items():
        print(key)
        firestorm = Tweets.from_query(
            query_dict["query"], filters=[equality_filter_factory("lang", "de")]
        )
        print(get_firestorm_wrapping_datetime(firestorm))
