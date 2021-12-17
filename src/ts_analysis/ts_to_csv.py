import csv

from src.db.connection import connect_to_mongo
from src.db.queried import QUERIES, query_iterator
from src.twitter_data.filters import default_filters_factory
from src.twitter_data.tweets import Tweets

from src.utils.output_folders import DATA_TIME_SERIES_FOLDER
from src.ts_analysis.hourly_statistics import (
    offensiveness_per_hour,
    tweet_quantity_per_hour,
)


def timeseries_summarized_to_csvs(
    aggr_file_name: str = DATA_TIME_SERIES_FOLDER + "aggression_ts.csv",
    quant_file_name: str = DATA_TIME_SERIES_FOLDER + "quantities_ts.csv",
):
    # Opening Files
    open_files = []

    aggr_file = open(aggr_file_name, "w")
    aggr_writer = csv.writer(aggr_file)
    open_files.append(aggr_file)

    quantity_file = open(quant_file_name, "w")
    quantity_writer = csv.writer(quantity_file)
    open_files.append(quantity_file)

    # Writing Data
    for key, query_dict in query_iterator(QUERIES, include_timeseries_disabled=False):
        # Loading the firestorm
        firestorm = Tweets.from_query(
            query_dict["query"], filters=default_filters_factory(query_dict)
        )
        aggr_writer.writerow([key] + offensiveness_per_hour(firestorm))
        quantity_writer.writerow([key] + tweet_quantity_per_hour(firestorm))

    # Closing Files
    for file in open_files:
        file.close()


if __name__ == "__main__":
    connect_to_mongo()

    timeseries_summarized_to_csvs()
