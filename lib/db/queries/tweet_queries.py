from lib.utils.regex_helpers import remove_leading_hashtag
from lib.db.schemes import Tweets
from lib.db.connection import connect_to_mongo
from json import loads

from mongoengine import QuerySet
from typing import Sequence, List


def get_tweets_for_hashtags(*hashtags: Sequence[str]) -> QuerySet:
    """Returns all tweets that contained one of the specified hashtags

    :param hashtags: list of hashtags to query for
    :return: mongoengine QuerySet that contains all tweets that contain at least one of the hashtags;
    """

    # Remove leading # symbol if they were included in the input
    hashtags = remove_leading_hashtag(hashtags)

    return Tweets.objects(entities__hashtags__tag__in=hashtags)


def get_tweets_for_search_query(hashtag: str) -> QuerySet:
    """Returns all tweets that were returned for a query that contains the specified hashtag

    :param hashtags: a hashtag which was queried
    :return: mongoengine QuerySet that contains all tweets that were retrieved when querying for this hashtag
    """

    # Remove leading # symbol if they were included in the input
    hashtags = remove_leading_hashtag(hashtag)

    results = Tweets.objects(search_params__query__icontains=hashtag)

    if len(results) == 0:
        print(f'WARNING: Your query "{hashtag}" did not return any results!')

    return results


def get_tweets_for_search_query_sorted_by_time(hashtag: str) -> List[dict]:
    tweets_query_set_sorted = get_tweets_for_search_query(hashtag).order_by('created_at').only(
        'created_at', 'user_type', 'tweet_type', 'contains_url', 'lang'
    )
    return loads(tweets_query_set_sorted.to_json())


if __name__ == "__main__":
    connect_to_mongo()
    print(len(get_tweets_for_search_query('#pinkyglove')))
