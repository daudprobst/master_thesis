import json
from time import sleep

import pandas as pd
from mongoengine import QuerySet

from src.db.connection import connect_to_mongo
from src.db.queries.tweet_queries import get_tweets_for_search_query


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


def delete_tweets(search_query: str) -> None:
    """Handle with care! Deletes all tweets returned for the search query

    :param search_query: search query for the tweets you want to deleted
    """

    tweets_to_delete = get_tweets_for_search_query(
        search_query, full_match_required=True
    )
    print(
        f'WARNING: {len(tweets_to_delete)} tweets will be deleted for query "{search_query}". Stop the program now '
        f"in case you want to cancel the deletion."
    )
    for i in reversed(range(10)):
        print(i)
        sleep(1)
    print("Deleting...")
    tweets_to_delete.delete()


if __name__ == "__main__":
    connect_to_mongo()
    delete_tweets(
        "#HelmeRettenLeben OR #lookslikeshit OR #saveslifes OR conversation_id:1108842805089177615"
    )
