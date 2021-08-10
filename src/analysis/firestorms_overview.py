from lib.twitter_data.tweets import Tweets
from lib.db.queried import QUERIES
from lib.db.connection import connect_to_mongo
from lib.utils.conversions import float_to_pct
from datetime import datetime
import csv
from typing import List


def tweet_quantity_per_hour(query_dict: dict) -> List[int]:
    """ Returns the quantity of tweets per hour (only in true_data time range)

    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: list of total_tweets per hour for each hour in firestorm
    """

    firestorm = Tweets.from_query(query_dict['query']).select_time_range(
        query_dict['true_start_date'], query_dict['true_end_date']
    )

    return list(firestorm.hourwise_metrics['total_tweets'].astype('int64'))


def get_firestorms_metadata(query_dict: dict) -> dict:
    """Returns a selection of metadata for the firestorm described by the query
    :param query_dict: the query dict containing the query itself as well as the end and start date
    :return: A dictionary containing metadata such as length, average_aggression, start and end data for the firestorm
    """

    firestorm = Tweets.from_query(query_dict['query'])

    raw_data_length = len(firestorm)
    firestorm = firestorm.select_time_range(query_dict['true_start_date'], query_dict['true_end_date'])
    value_counts = firestorm.tweets.is_offensive.value_counts().to_dict()

    output_dict = {
        'length': len(firestorm),
        'length_without_cut': raw_data_length,
        'pct_cut_out': float_to_pct((1 - len(firestorm)/raw_data_length)),
        'average_aggressiveness':  float_to_pct(value_counts[True] / (value_counts[True] + value_counts[False])),
        'aggr_value_counts': value_counts
    }

    output_dict.update(query_dict)

    # parsing datetime to more easily readable format
    for key, val in output_dict.items():
        if isinstance(val, datetime):
            # we only take in full days in the dataset so it is sufficient to log just the date
            output_dict[key] = val.date().isoformat()

    return output_dict


if __name__ == "__main__":
    connect_to_mongo()
    with open('/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms_quantities.csv', 'w') as f:
        writer = csv.writer(f)
        for key, query_dict in QUERIES.items():
            quantities = tweet_quantity_per_hour(query_dict)
            print([key] + quantities)
            writer.writerow([key] + quantities)



    '''
    connect_to_mongo()
    with open('/home/david/Desktop/Masterarbeit/twit_scrape/data/firestorms_overview.csv', 'w') as f:
        writer = csv.writer(f)
        # just querying a small firestorm to write the headers
        writer.writerow(get_firestorms_metadata(QUERIES['helmeLookLikeShit']))
        # loading the actual values
        for key, query_dict in QUERIES.items():
            print(f'Processing {key}')
            firestorm_summary = get_firestorms_metadata(query_dict)
            print(firestorm_summary)
            writer.writerow(firestorm_summary.values())
    '''

