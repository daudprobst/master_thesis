import csv
from datetime import datetime

from src.db.connection import connect_to_mongo
from src.db.queried import query_iterator, QUERIES
from src.ts_analysis.hourly_statistics import tweet_quantity_per_hour
from src.twitter_data.filters import de_filter
from src.twitter_data.tweets import Tweets
from src.utils.output_folders import DATA_BASE_FOLDER, DATA_RAW_QUANTS_FOLDER


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
    queries: dict = QUERIES,
    output_filename: str = f"{DATA_RAW_QUANTS_FOLDER}/raw_quantities.csv",
):
    with open(output_filename, "w") as f:
        writer = csv.writer(f)
        for key, query_dict in query_iterator(queries):
            firestorm = Tweets.from_query(query_dict["query"], filters=[de_filter()])
            writer.writerow([key] + tweet_quantity_per_hour(firestorm))


def firestorm_overview_to_csv(
    queries: dict = QUERIES,
    output_filename: str = f"{DATA_BASE_FOLDER}/data/firestorms_overview.csv",
):
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


if __name__ == "__main__":
    connect_to_mongo()
    # firestorm_overview_to_csv()
    hourly_quantities_to_csv()
