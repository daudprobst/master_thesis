import pandas as pd
from json import loads
from datetime import datetime
from lib.db.queries.tweet_queries import get_tweets_for_search_query
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour, round_to_hour_slots
from typing import Sequence, Tuple
from lib.graphs.line_plots import smoothed_line_plots


class Tweets:

    def __init__(self, tweets: pd.DataFrame):
        self._tweets = self._preprocess_inputs(tweets)
        self._hourwise_metrics = self.metrics_per_time_intervall('hour')
        self._six_hourwise_metrics = self.metrics_per_time_intervall('six_hour_slot')

    def __len__(self):
        return len(self.tweets)


    @classmethod
    def from_query(cls, fetch_query: str, full_match_required:bool=True):
        """Returns all tweets that were fetched by making use of the specified query

        :param fetch_query: the fetch_query for which tweets should be returned (query originally used for fetching!)
        :param full_match_required: if false, tweets are returned if they were fetched with fetch_query
        if true the fetch_query must be fully equal to the query used for fetching the tweets
        :return: all tweets who were fetched with fetch_query
        """
        firestorm_tweets_selection = loads(
            get_tweets_for_search_query(fetch_query, full_match_required=full_match_required).to_json())
        return cls(pd.DataFrame.from_records(firestorm_tweets_selection))


    @property
    def tweets(self):
        return self._tweets

    @property
    def hourwise_metrics(self):
        return self._hourwise_metrics

    @property
    def six_hourwise_metrics(self):
        return self._six_hourwise_metrics

    def select_time_range(self, start_time: datetime, end_time: datetime,
                          time_variable: str = 'created_at'):
        """ returns only those tweets that lie in the specified time range

        :param start_point: starting point (inclusive) of the time range (only tweets after this point are returned)
        :param end_point: end point (exclusive) of the time range (only tweets before this point are returned)
        :param time_variable: time variable by which the selection should happen (defaults to 'created_at', e.g. 'hour'
        migh also make sense in some instances)
        :return: tweets after start_point AND before end_point
        """

        # TODO smarter solution for this tz issue / Which tz are the tweets stored in? Probably we should cast to UTC?

        if start_time.tzinfo:
            print('WARNING: Ignored tz information for selecting range of tweets')
            start_time = start_time.replace(tzinfo=None)
        if end_time.tzinfo:
            print('WARNING: Ignored tz information for selecting range of tweets')
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
            tweets['six_hour_slot'] = tweets['created_at'].apply(lambda x: round_to_hour_slots(x))
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

    def metrics_per_time_intervall(self, grouping_var: str = 'hour',
                                    to_calculate: Sequence[Tuple[str, str, str]] = None) -> pd.DataFrame:
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
                # ==offensiveness
                ("offensive_pct", 'is_offensive', True), #CAREFUL WITH THE NONE VALUES!!!#
                ("not_offensive_pct", 'is_offensive', False)  # FOR TESTING AROUND WITH NONE VALUES
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
            output_df.at[name, 'total_tweets'] = total_length
            for col, var_name, value in to_calculate:
                try:
                    output_df.at[name, col] = group[var_name].value_counts()[value] / total_length
                except KeyError: # sometimes value does not exist in value_counts (0 entries)
                    output_df.at[name, col] = 0

        # Normalizing total length (only works since data has only positive values)
        output_df['total_tweets_pct'] = output_df['total_tweets'] / output_df['total_tweets'].max()

        # sort the output by time (hour)
        return output_df.sort_index()

    def plot_quantity_per_hour(self, **kwargs) -> None:
        smoothed_line_plots(self.hourwise_metrics, x='hour', y=['total_tweets'], window_size=0, **kwargs).show()