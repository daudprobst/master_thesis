import ruptures as rpt
from lib.db.connection import connect_to_mongo
from lib.db.queries.tweet_queries import get_tweets_for_search_query_sorted_by_time
from lib.preprocessing.preprocess_tweets_df import select_time_range, preprocess_tweets_df, rates_per_hour
from datetime import datetime
import pandas as pd
from typing import List
from lib.graphs.pie_plot import pie_plot_multiplot


def get_breakpoints(tweets: pd.DataFrame) -> List[datetime]:

    # casting to categorical

    # == Transform tweets to per-hour information on the percentage that certain attributes make up in the data set
    # e.g. 60% retweets in the first hour of the firestorm

    # vars to calculate percentages for: (colname, variable_name, value)
    to_calculate = [
        # ("total_tweets", None, None),
        ("retweet_pct", 'tweet_type', 'retweet without comment'),

        # ("quoted_pct", 'tweet_type', 'retweet with comment'),
        ("original_tweet_pct", 'tweet_type', 'original tweet'),
        ("reply_pct", 'tweet_type', 'reply'),
        ("laggards_pct", 'user_type', 'laggard'),
        # ("hyper_active_pct", 'user_type', 'hyper-active'),
        ("active_pct", 'user_type', 'active'),
        ("de_pct", 'lang', 'de'),
        # ("en_pct", 'lang', 'en'),
    ]

    metrics_per_hour = rates_per_hour(tweets, to_calculate)

    # ready to calculate the breakpoints!
    # transform data in structure that works with the ruptures package

    # TODO is using only n-1 attributes for each category correct to avoid multicollinearity?
    VARIABLES_FOR_BREAKPOINT_ANALYSIS = [
        'retweet_pct', 'original_tweet_pct', 'reply_pct',
        'laggards_pct', 'active_pct',
    ]

    signal = metrics_per_hour[VARIABLES_FOR_BREAKPOINT_ANALYSIS].to_numpy()
    print(f'Calculating breakpoints for signal of shape {signal.shape}')

    # Specify the model params
    algo = rpt.Pelt(model='rbf').fit(signal)
    # TODO tweak penalty
    result = algo.predict(pen=3)

    # result from ruptures typically contains one breakpoint all the way after the dataset, e.g. [205, 480] for a
    # TODO CHECK WHETHER THE BREAKPOINT IS TO BE INCLUDED OR EXCLUDED!!!
    bkp_in_data = [x-1 for x in result]
    print(bkp_in_data)
    return [x.to_pydatetime() for x in metrics_per_hour.index[bkp_in_data]]

def split_tweets_at_breakpoints(tweets: pd.DataFrame) -> List[pd.DataFrame]:
    """ Determines the breakpoints for the specified tweets and splits the tweets at these positions

    :param tweets: tweets to divide into their phases
    :return: list of dfs, where each df contains all tweets in a phase
    """
    # Get Breakpoints
    breakpoints = get_breakpoints(tweets)
    print(breakpoints)
    # last breakpoints seems to be irrelevant as it is just the last tweet
    breakpoints.pop()

    # Segmenting the original data into dfs

    # Add one breakpoints all the way in the beginning and one all the way in the end
    breakpoints_wrapped = [tweets['hour'].min().to_pydatetime()] + \
                          breakpoints + [tweets['hour'].max().to_pydatetime()]

    return [select_time_range(tweets, breakpoints_wrapped[i], breakpoints_wrapped[i+1], 'hour')
                   for i in range(len(breakpoints_wrapped) - 1)]

if __name__ == "__main__":
    #==== Importing Tweets ====#
    connect_to_mongo()
    input_tweets = preprocess_tweets_df(pd.DataFrame(get_tweets_for_search_query_sorted_by_time('pinkygloves')))
    print(f'Before selecting the timerange, there are {len(input_tweets)} in the dataset')

    # Opt: Only use tweets from specified time range (en wave happens between 17th and 23!)
    input_tweets = select_time_range(input_tweets, datetime.strptime("2021-04-13 12:00:00", '%Y-%m-%d %H:%M:%S'),
                                     datetime.strptime("2021-04-17 12:00:00", '%Y-%m-%d %H:%M:%S'))
    print(f'After selecting the timerange, there are {len(input_tweets)} in the dataset')
    # ================#

    phases_dfs = split_tweets_at_breakpoints(input_tweets)

    # Plotting!
    pie_plot_multiplot(phases_dfs, attributes_to_plot=['tweet_type', 'user_type', 'lang']).show()
