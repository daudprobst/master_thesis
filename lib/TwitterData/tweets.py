import pandas as pd
from json import loads

from lib.db.queries.tweet_queries import get_tweets_for_search_query


class Tweets:

    def __init__(self, tweets: pd.DataFrame):
        self._tweets = tweets

    @classmethod
    def from_hashtag_in_query(cls, hashtag: str):
        firestorm_tweets_selection = loads(get_tweets_for_search_query('pinkygloves').to_json())
        cls(pd.DataFrame.from_records(firestorm_tweets_selection))

    def select_time_range(self, start_time, end_time):
        pass

    def _preprocess_inputs(self):
