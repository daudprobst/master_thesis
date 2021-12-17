from mongoengine import QuerySet
from src.db.connection import connect_to_mongo
from src.db.schemes import Tweets
from src.db.helpers import query_set_to_df
from datetime import datetime


def get_tweet_for_id(tweet_id) -> QuerySet:
    """Returns the tweet for the id (Query Set of length 1 if the tweet exists, length 0 if it does not exist)"""
    return Tweets.objects.get(id=tweet_id)


def get_tweets_for_ids(tweet_ids: list[int]) -> QuerySet:
    """Returns all matching tweets for the list of ids"""
    return Tweets.objects(id__in=tweet_ids)


def get_tweets_for_search_query(
    query: str, full_match_required: bool = False
) -> QuerySet:
    """Returns all tweets that were returned for a query that contains the specified hashtag

    :param query: the query (or a part of the query) that was used for fetching the tweets
    :param full_match_required: if true tweets are only returned if their query is equal to the input query, if false
    it suffices if the input query is a part of the query used for fetching the tweet
    :return: mongoengine QuerySet that contains all tweets that were retrieved when querying for this hashtag
    """
    if full_match_required:
        results = Tweets.objects(search_params__query=query)
    else:
        results = Tweets.objects(search_params__query__icontains=query)

    if len(results) == 0:
        print(f'WARNING: Your query "{query}" did not return any results!')

    return results


def tweets_after_date(end_datetime: datetime, query: str) -> QuerySet:
    """Returns all tweets matching the query that occured before end_datetime - Extremely inefficient implementation!

    :param end_datetime: Tweets after this datetime are excluded
    :param query: Query for the tweets that should be included
    :return: Tweets that match the query and occured before end_datetime
    """
    tweets_matching_query = get_tweets_for_search_query(query)
    # transform query_set to df and filter it
    tweets_df = query_set_to_df(tweets_matching_query)
    end_date_timestamp = end_datetime.timestamp() * 1000  # timestamp in ms
    tweets_after_timestamp = tweets_df[
        tweets_df["created_at"].apply(lambda x: x["$date"] > end_date_timestamp)
    ]

    # Transform back to query set
    tweets_after_timestamp_query_set = get_tweets_for_ids(
        list(tweets_after_timestamp["_id"])
    )
    return tweets_after_timestamp_query_set


if __name__ == "__main__":
    connect_to_mongo()
    print(len(get_tweets_for_search_query("#pinkyglove")))
