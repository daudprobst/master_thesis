import csv
import os
from datetime import datetime

from src.db.connection import connect_to_mongo
from src.db.queried import QUERIES
from src.twitter_data.filters import default_filters_factory
from src.twitter_data.tweets import Tweets
from src.utils.conversions import float_to_pct


def offensiveness_per_hour(query_dict: list[dict]) -> list[float]:
    """Returns the offensiveness in the discourse of the firestorm per hour to file_path (only in true_data time range)

    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: percentage of offensive tweets per hour of the discourse in the queried firestorm (rounded to 2 decimals)
    """

    firestorm = Tweets.from_query(
        query_dict["query"], filters=default_filters_factory(query_dict)
    )

    offensiveness_pct_raw = list(firestorm.hourwise_metrics["offensive_pct"])
    return [round(offensiveness_pct, 4) for offensiveness_pct in offensiveness_pct_raw]


def not_offensiveness_per_hour(query_dict: list[dict]) -> list[float]:
    """TODO Remove"""

    firestorm = Tweets.from_query(
        query_dict["query"], filters=default_filters_factory(query_dict)
    )

    offensiveness_pct_raw = list(firestorm.hourwise_metrics["not_offensive_pct"])
    return [round(offensiveness_pct, 4) for offensiveness_pct in offensiveness_pct_raw]


def tweet_quantity_per_hour(query_dict: dict) -> list[int]:
    """Returns the quantity of tweets per hour (only in true_data time range)

    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: list of total_tweets per hour for each hour in firestorm
    """

    firestorm = Tweets.from_query(
        query_dict["query"], filters=default_filters_factory(query_dict)
    )

    return list(firestorm.hourwise_metrics["total_tweets"].astype("int64"))


def get_firestorms_metadata(query_dict: dict) -> dict:
    """Returns a selection of metadata for the firestorm described by the query
    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: A dictionary containing metadata such as length, average_aggression, start and end data for the firestorm
    """

    firestorm = Tweets.from_query(
        query_dict["query"], filters=default_filters_factory(query_dict)
    )

    filter_log = firestorm.filter_log

    value_counts = firestorm.tweets.is_offensive.value_counts().to_dict()

    output_dict = {
        "length": len(firestorm),
        "filtering_lengths_log": filter_log,
        "pct_filtered": float_to_pct(1 - (filter_log[0] / filter_log[-1])),
        "average_aggressiveness": float_to_pct(
            value_counts[True] / (value_counts[True] + value_counts[False])
        ),
        "aggr_value_counts": value_counts,
    }

    output_dict.update(query_dict)

    # parsing datetime to more easily readable format
    for key, val in output_dict.items():
        if isinstance(val, datetime):
            # we only take in full days in the dataset so it is sufficient to log just the date
            output_dict[key] = val.date().isoformat()

    return output_dict


if __name__ == "__main__":
    """
    connect_to_mongo()
    with open(
        "/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms_quantities.csv",
        "w",
    ) as f:
        writer = csv.writer(f)
        for key, query_dict in QUERIES.items():
            quantities = tweet_quantity_per_hour(query_dict)
            print([key] + quantities)
            writer.writerow([key] + quantities)
    """

    connect_to_mongo()
    with open(os.getcwd() + "/data/firestorm_aggressions2.csv", "w") as f:
        writer = csv.writer(f)
        for key, query_dict in list(QUERIES.items())[0:1]:  # TODO remove selection
            quantities = offensiveness_per_hour(query_dict)
            print([key] + quantities)
            writer.writerow([key] + quantities)
            anti_quants = not_offensiveness_per_hour(query_dict)
            writer.writerow(["NEG" + key] + anti_quants)

    """
    connect_to_mongo()
    with open('/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms_overview.csv', 'w') as f:
        writer = csv.writer(f)
        # just querying a random firestorm to write the headers
        writer.writerow(get_firestorms_metadata(list(QUERIES.values())[0]))
        # loading the actual values
        for key, query_dict in QUERIES.items():
            print(f'Processing {key}')
            firestorm_summary = get_firestorms_metadata(query_dict)
            print(firestorm_summary)
            writer.writerow(firestorm_summary.values())
    """
