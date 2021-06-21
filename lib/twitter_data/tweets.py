import pandas as pd
from json import loads
from datetime import datetime
from lib.db.queries.tweet_queries import get_tweets_for_search_query
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour
from typing import Sequence, Tuple

class Tweets:

    def __init__(self, tweets: pd.DataFrame):
        self._tweets = self._preprocess_inputs(tweets)
        self._hourwise_metrics = self._rates_per_hour()

    def __len__(self):
        return len(self.tweets)

    @classmethod
    def from_hashtag_in_query(cls, hashtag: str):
        firestorm_tweets_selection = loads(get_tweets_for_search_query(hashtag).to_json())
        return cls(pd.DataFrame.from_records(firestorm_tweets_selection))


    @property
    def tweets(self):
        return self._tweets

    @property
    def hourwise_metrics(self):
        return self._hourwise_metrics

    def select_time_range(self, start_time: datetime, end_time: datetime,
                          time_variable: str = 'created_at'):
        """ returns only those tweets that lie in the specified time range

        :param start_point: starting point (inclusive) of the time range (only tweets after this point are returned)
        :param end_point: end point (exclusive) of the time range (only tweets before this point are returned)
        :param time_variable: time variable by which the selection should happen (defaults to 'created_at', e.g. 'hour'
        migh also make sense in some instances)
        :return: tweets after start_point AND before end_point
        """

        # TODO at least throw a warning if we do this
        # remove timezone information


        if start_time.tzinfo:
            print('WARNING: Ignored tz informaiton for selecting range of tweets')
            start_time = start_time.replace(tzinfo=None)
        if end_time.tzinfo:
            print('WARNING: Ignored tz informaiton for selecting range of tweets')
            end_time = end_time.replace(tzinfo=None)

        return self.__class__(
            self.tweets[(self.tweets[time_variable] >= start_time) & (self.tweets[time_variable] < end_time)]
        )

    # TODO there must be a smarter way to solve this inheritance issue!
    def select_tweets_in_time_range(self, start_time: datetime, end_time: datetime,
                          time_variable: str = 'created_at'):

        return Tweets(
            self.tweets[(self.tweets[time_variable] >= start_time) & (self.tweets[time_variable] < end_time)]
        )

    def _preprocess_inputs(self, tweets) -> pd.DataFrame:
        """ Parses created_at to datetime, adds hour attributes and casts categorical variables (e.g. tweet type) to
         categorical dytpe

         :param tweets: tweets to preprocess
         :return: preprocessed tweets
         """
        TWEET_TYPE_LEVELS = pd.api.types.CategoricalDtype(categories=["retweet with comment", "retweet without comment",
                                                                      "reply", "original tweet"])

        USER_TYPE_LEVELS = pd.api.types.CategoricalDtype(categories=["laggard", "hyper-active", "active"])

        # adding date attributes
        try:
            tweets['created_at'] = tweets['created_at'].apply(lambda x: unix_ms_to_date(x['$date']))
            tweets['hour'] = tweets['created_at'].apply(lambda x: round_to_hour(x))
        except Exception:
            # TODO -> implement more clean: current solution is very dirty. the upper code fails if we had already
            # parased the dates before
            pass

        # casting attributes to categorical
        if str(tweets.dtypes['tweet_type']) != 'category':
            tweets["tweet_type"] = tweets["tweet_type"].astype(TWEET_TYPE_LEVELS)
        if str(tweets.dtypes['user_type']) != 'category':
            tweets['user_type'] = tweets['user_type'].astype(USER_TYPE_LEVELS)
        if str(tweets.dtypes['lang']) != 'category':
            tweets['lang'] = tweets['lang'].astype("category")

        return tweets

    def _rates_per_hour(self, to_calculate: Sequence[Tuple[str, str, str]] = None,
                       grouping_var: str = 'hour') -> pd.DataFrame:
        """ Groups the tweets per hour and calculates percentages for certain values in categorical variables that
        were specified in to_calculate (e.g. ("retweet_pct", 'tweet_type', 'retweet without comment') will include the
        percentage of retweets without comment for each hour in the column 'retweet_pct')

        :param grouping_var: variable by which the grouping should occur (e.g. hour)
        :param to_calculate: variables to calculate for each hour; each output is described by a tuple with three entries:
        1. colname: name that the output column should have (e.g. 'retweet_pct')
        2. variable_name: name of variable in input column (e.g. 'tweet_type')
        3. value: value for which the rate should be calculated (e.g. 'retweet without comment')
        :return: data frame with metrics(rates) for each hour for some variables
        """

        if not to_calculate:
            to_calculate = [
                ("total_tweets", None, None),
                # ==tweet type
                ("retweet_pct", 'tweet_type', 'retweet without comment'),
                ("original_tweet_pct", 'tweet_type', 'original tweet'),
                ("reply_pct", 'tweet_type', 'reply'),
                ("quoted_pct", 'tweet_type', 'retweet with comment'),
                # ==user type
                ("laggards_pct", 'user_type', 'laggard'),
                ("active_pct", 'user_type', 'active'),
                ("hyper_active_pct", 'user_type', 'hyper-active'),
                # ==lang
                ("de_pct", 'lang', 'de'),
                ("en_pct", 'lang', 'en'),
            ]

        # setting up the output_df
        df_index = self.tweets[grouping_var].unique()
        cols = [x[0] for x in to_calculate]
        output_df = pd.DataFrame(columns=cols, index=df_index)
        output_df[grouping_var] = output_df.index  # also have index as column (often useful for plotting)

        # group all tweets that appeared in the same hour and calculate stats for them
        tweets_by_hour = self.tweets.groupby(grouping_var)
        for name, group in tweets_by_hour:
            total_length = len(group)
            for col, var_name, value in to_calculate:
                if col == 'total_tweets':
                    output_df.at[name, 'total_tweets'] = total_length
                else:
                    output_df.at[name, col] = group[var_name].value_counts()[value] / total_length

        if 'total_tweets' in output_df.columns:
            # Normalizing total length (only works since data has only positive values)
            output_df['total_tweets'] = output_df['total_tweets'] / output_df['total_tweets'].max()

        # sort the output by time (hour)
        return output_df.sort_index()

