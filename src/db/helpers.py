import json
from time import sleep

import pandas as pd
from mongoengine import QuerySet

from src.db.connection import connect_to_mongo
from src.db.queries.tweet_queries import get_tweets_for_search_query, get_tweets_for_ids
from datetime import datetime
from dateutil import parser


def query_set_to_df(input_data: QuerySet) -> pd.DataFrame:
    return pd.DataFrame.from_records(json.loads(input_data.to_json()))


def update_search_query(old_query: str, full_new_query: str) -> None:
    """

    :param old_query: part of old query; All tweets whose query included this part will be changed
    :param full_new_query: new query that these tweets are set to
    """
    tweets_new_query_before_update = get_tweets_for_search_query(full_new_query)
    tweets_old_query = get_tweets_for_search_query(old_query)

    for tweet in tweets_old_query:
        tweet.update(search_params__query=full_new_query)
    updated_tweets = get_tweets_for_search_query(full_new_query)

    print(
        f"Before the update there were {len(tweets_new_query_before_update)} tweets for the new query {full_new_query}"
        f"and {len(tweets_old_query)} tweets for the old query {old_query}."
        f"After the update the new query returns {len(updated_tweets)} tweets."
        f'And {len(get_tweets_for_search_query("Klöckner"))} tweets. for the query "Klöckner"'
    )


def delete_tweets(tweets_to_delete: QuerySet) -> None:
    """Handle with care! Deletes all tweets that are passed in the database

    :param tweets_to_delete: QuerySet of tweets that should be deleted
    """

    print(
        f'WARNING: {len(tweets_to_delete)} tweets will be deleted.'
        f"Interrupt the program now if you want to cancel the deletion."
    )
    for i in reversed(range(10)):
        print(i)
        sleep(1)
    print("Deleting...")
    tweets_to_delete.delete()


def tweets_after_date(end_datetime: datetime, query: str):
    tweets_matching_query = get_tweets_for_search_query(query)
    # transform query_set to df and filter it
    tweets_df = query_set_to_df(tweets_matching_query)
    end_date_timestamp = end_datetime.timestamp() * 1000  # timestamp in ms
    tweets_after_timestamp = tweets_df[
        tweets_df["created_at"].apply(lambda x: x["$date"] > end_date_timestamp)
    ]

    # Transform back to query set
    tweets_after_timestamp_query_set = get_tweets_for_ids(list(tweets_after_timestamp['_id']))
    return tweets_after_timestamp_query_set


if __name__ == "__main__":
    connect_to_mongo()

    query = "#pinkygloves OR #pinkygate"

    tweets_to_delete = tweets_after_date(parser.parse("2021-04-26"), query)
    delete_tweets(tweets_to_delete)
