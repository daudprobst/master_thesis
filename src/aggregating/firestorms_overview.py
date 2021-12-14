import csv
import os
from datetime import datetime

from src.db.connection import connect_to_mongo
from src.db.queried import QUERIES
from src.twitter_data.filters import default_filters_factory
from src.twitter_data.tweets import Tweets


def tweet_quantity_per_hour(firestorm: Tweets) -> list[int]:
    """Returns the quantity of tweets per hour (only in true_data time range)

    :param firestorm: collection of tweets for which the tweet quantity should be calculated
    :return: list of total_tweets per hour for each hour in firestorm
    """

    return list(firestorm.hourwise_metrics["total_tweets"].astype("int64"))


def offensiveness_per_hour(firestorm: Tweets) -> list[float]:
    """Returns the offensiveness in the discourse of the firestorm per hour to file_path (only in true_data time range)

     :param firestorm: collection of tweets for which the offensiveness should be calculated
    :return: percentage of offensive tweets per hour of the discourse in the queried firestorm (rounded to 2 decimals)
    """
    offensiveness_pct_raw = list(firestorm.hourwise_metrics["offensive_pct"])
    return [round(offensiveness_pct, 4) for offensiveness_pct in offensiveness_pct_raw]


def get_firestorms_metadata(firestorm: Tweets, query_dict: dict) -> dict:
    """Returns a selection of metadata for the firestorm described by the query
    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: A dictionary containing metadata such as length, average_aggression, start and end data for the firestorm
    """

    output_dict = firestorm.metadata()
    output_dict.update(query_dict)

    # parsing datetime to more easily readable format
    for key, val in output_dict.items():
        if isinstance(val, datetime):
            output_dict[key] = val.isoformat()

    return output_dict


def firestorms_summarized_to_csvs(query_dicts: dict, write_settings: dict = None):
    query_dicts = list(query_dicts.items())
    # Opening Files
    open_files = []
    if write_settings["aggression"]["enabled"]:
        aggr_file = open(write_settings["aggression"]["file_name"], "w")
        aggr_writer = csv.writer(aggr_file)
        open_files.append(aggr_file)

    if write_settings["quantities"]["enabled"]:
        quantity_file = open(write_settings["quantities"]["file_name"], "w")
        quantity_writer = csv.writer(quantity_file)
        open_files.append(quantity_file)

    if write_settings["overview"]["enabled"]:
        overview_file = open(write_settings["overview"]["file_name"], "w")
        overview_writer = csv.writer(overview_file)
        open_files.append(overview_file)

    # Writing Headers
    if write_settings["overview"]["enabled"]:
        first_query_dict = query_dicts[0][1]
        first_firestorm = Tweets.from_query(
            first_query_dict["query"], filters=default_filters_factory(first_query_dict)
        )
        overview_writer.writerow(
            ["key"]
            + list(get_firestorms_metadata(first_firestorm, first_query_dict).keys())
        )

    # Writing Data
    for key, query_dict in query_dicts:
        # Loading the firestorm
        print(f"Processing {key}")
        firestorm = Tweets.from_query(
            query_dict["query"], filters=default_filters_factory(query_dict)
        )
        if write_settings["aggression"]["enabled"]:
            aggr_writer.writerow([key] + offensiveness_per_hour(firestorm))

        if write_settings["quantities"]["enabled"]:
            quantity_writer.writerow([key] + tweet_quantity_per_hour(firestorm))

        if write_settings["overview"]["enabled"]:
            firestorm_summary = get_firestorms_metadata(firestorm, query_dict)
            print(firestorm_summary)
            overview_writer.writerow([key] + list(firestorm_summary.values()))

    # Closing Files
    for file in open_files:
        file.close()


if __name__ == "__main__":
    connect_to_mongo()

    query_dicts = {
        key: entry
        for (key, entry) in QUERIES.items()
        if not ("disabled" in entry and entry["disabled"])
    }
    write_settings = {
        "aggression": {
            "enabled": True,
            "file_name": os.getcwd() + "/data/firestorm_aggressions.csv",
        },
        "quantities": {
            "enabled": True,
            "file_name": os.getcwd() + "/data/firestorm_quantities.csv",
        },
        "overview": {
            "enabled": True,
            "file_name": os.getcwd() + "/data/firestorm_overview.csv",
        },
    }

    firestorms_summarized_to_csvs(query_dicts, write_settings)
