from time import sleep

from mongoengine import QuerySet
from src.db.schemes import Tweets
from src.db.tweet_queries import get_tweets_for_search_query


def add_attribute_to_tweet(tweet: Tweets, attribute: str, value) -> None:
    """Sets an attribute for a specific tweet to a value
    :param tweet: tweet to update
    :param attribute: attribute to update for this tweet
    :param value: value that this attribute should be set to
    """
    if attribute == "tweet_type":
        return tweet.update(set__tweet_type=value)
    elif attribute == "contains_url":
        return tweet.update(set__contains_url=value)
    elif attribute == "user_type":
        return tweet.update(set__user_type=value)
    elif attribute == "is_offensive":
        return tweet.update(set__is_offensive=value)
    elif attribute == "user_activity":
        return tweet.update(set__user_activity=value)
    elif attribute == "firestorm_activity":
        return tweet.update(set__firestorm_activity=value)
    else:
        raise ValueError(
            f"No know operation exists for adding the attribute {attribute}"
        )


def update_search_query(tweets_to_update: QuerySet, full_new_query: str) -> None:
    """Updates the query_parameter of the tweets
    :param tweets_to_update: The tweets for which the query_parameter should be updated
    :param full_new_query: new query that these tweets are set to
    """
    tweets_new_query_before_update = get_tweets_for_search_query(full_new_query)

    for tweet in tweets_to_update:
        tweet.update(search_params__query=full_new_query)
    updated_tweets = get_tweets_for_search_query(full_new_query)

    print(
        f"Before the update there were {len(tweets_new_query_before_update)} tweets for the new query {full_new_query}"
        f"and {len(tweets_to_update)} tweets for the old query."
        f"After the update the new query returns {len(updated_tweets)} tweets."
    )


def delete_tweets(tweets_to_delete: QuerySet) -> None:
    """Handle with care! Deletes all tweets that are passed in the database

    :param tweets_to_delete: QuerySet of tweets that should be deleted
    """

    print(
        f"WARNING: {len(tweets_to_delete)} tweets will be deleted."
        f"Interrupt the program now if you want to cancel the deletion."
    )
    for i in reversed(range(10)):
        print(i)
        sleep(1)
    print("Deleting...")
    tweets_to_delete.delete()
