from lib.utils.regex_helpers import remove_leading_hashtag
from lib.db.schemes import Tweets
from lib.db.connection import connect_to_mongo

from mongoengine import QuerySet
from typing import Sequence


def get_tweets_for_hashtags(*hashtags: Sequence[str]) -> QuerySet:
    """Returns all tweets that contained one of the specified hashtags

    :param hashtags: list of hashtags to query for
    :return: mongoengine QuerySet that contains all tweets that contain at least one of the hashtags;
    schema of tweets as defined in db.schemes.Tweets
    """

    # Remove leading # symbol if they were included in the input
    hashtags = remove_leading_hashtag(hashtags)

    # TODO for now this onyl retrieves the tweets for the first hashtag!
    return Tweets.objects(search_params__query__icontains=hashtags[0])


if __name__ == "__main__":
    connect_to_mongo()
    print(len(get_tweets_for_hashtags("pinkygloves")))
