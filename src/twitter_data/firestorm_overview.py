import csv
from datetime import datetime

from src.db.connection import connect_to_mongo
from src.db.queried import query_iterator, QUERIES
from src.ts_analysis.ts_to_csv import tweet_quantity_per_hour
from src.twitter_data.filters import de_filter
from src.twitter_data.tweets import Tweets
from src.utils.output_folders import DATA_BASE_FOLDER, DATA_TIME_SERIES_FOLDER


import os
import csv
from statistics import mean


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


def hourly_quantities_to_csv(
    output_filename: str = DATA_TIME_SERIES_FOLDER
    + "firestorms_raw_tweet_quantities.csv",
    queries: dict = QUERIES,
) -> None:
    """Writes the hourly quantity of tweets for each of the firestorms to the output file -> time series of tweet quantities

    :param output_filename: filename (csv) to which the output timeseries should be written
    :param queries: query_dicts for firestorms that should be included
    """
    with open(output_filename, "w") as f:
        writer = csv.writer(f)
        for key, query_dict in query_iterator(queries):
            firestorm = Tweets.from_query(query_dict["query"], filters=[de_filter()])
            writer.writerow([key] + tweet_quantity_per_hour(firestorm))


def firestorm_overview_to_csv(
    output_filename: str = f"{DATA_BASE_FOLDER}/firestorms_overview.csv",
    queries: dict = QUERIES,
) -> None:
    """For each firestorm generates some descriptive statistics and overview data and writes them to a csv

    :param output_filename: filename (csv) to which the firestorm overview should be written
    :param queries: query_dicts for firestorms that should be included
    """
    with open(output_filename, "w") as f:
        writer = csv.writer(f)
        # Write Headers
        first_query_dict = list(queries.items())[0][1]
        first_firestorm = Tweets.from_query(
            first_query_dict["query"], filters=[de_filter()]
        )
        writer.writerow(
            ["key"]
            + list(get_firestorms_metadata(first_firestorm, first_query_dict).keys())
        )
        # Write Body
        for key, query_dict in query_iterator(queries):
            firestorm = Tweets.from_query(query_dict["query"], filters=[de_filter()])
            firestorm_summary = get_firestorms_metadata(firestorm, query_dict)
            writer.writerow([key] + list(firestorm_summary.values()))


def calculate_filter_cuts(
    file_name=os.getcwd() + "/data/firestorm_overview.csv",
) -> None:
    """Prints statics on the amount of tweets that were cut due to filtering steps"

    :param file_name: file name of a firestorms overview csv file
    """
    # Calculate cuts due to filtering for only germany
    with open(file_name, "r") as f:
        reader = csv.DictReader(f)

        cuts = []
        cuts2 = []
        for row in reader:
            firestorm_name = row["key"]
            filter_log = (
                row["filtering_lengths_log"]
                .replace("[", "")
                .replace("]", "")
                .split(",")
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


if __name__ == "__main__":
    connect_to_mongo()
    firestorm_overview_to_csv()
    hourly_quantities_to_csv()
