from datetime import datetime
from src.twitter_data.tweets import Tweets


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