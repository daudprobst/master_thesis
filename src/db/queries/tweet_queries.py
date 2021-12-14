from mongoengine import QuerySet
from src.db.connection import connect_to_mongo
from src.db.schemes import Tweets


def get_tweet_for_id(tweet_id) -> QuerySet:
    """Returns the tweet for the id (Query Set of length 1 if the tweet exists, length 0 if it does not exist)"""
    return Tweets.objects.get(id=tweet_id)


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


if __name__ == "__main__":
    connect_to_mongo()
    print(len(get_tweets_for_search_query("#pinkyglove")))
