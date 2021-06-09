import ruptures as rpt
from lib.db.connection import connect_to_mongo
from lib.db.queries.tweet_queries import get_tweets_for_search_query_sorted_by_time
from lib.preprocessing.preprocess_tweets_df import select_time_range, preprocess_tweets_df
from datetime import datetime
import pandas as pd
from typing import List
from lib.graphs.pie_plot import pie_plot_multiplot

import plotly.express as px


def get_breakpoints(tweets: pd.DataFrame) -> List[datetime]:

    # casting to categorical

    # == Transform tweets to per-hour information on the percentage that certain attributes make up in the data set
    # e.g. 60% retweets in the first hour of the firestorm

    # vars to calculate percentages for: (colname, variable_name, value)
    to_calculate = [
        # ("total_tweets", None, None),
        ("retweet_pct", 'tweet_type', 'retweet without comment'),
        # TODO using only n-1 attributes to avoid multicollinearity is correct
        # ("quoted_pct", 'tweet_type', 'retweet with comment'),
        ("original_tweet_pct", 'tweet_type', 'original tweet'),
        ("reply_pct", 'tweet_type', 'reply'),
        ("laggards_pct", 'user_type', 'laggard'),
        # ("hyper_active_pct", 'user_type', 'hyper-active'),
        ("active_pct", 'user_type', 'active'),
        ("de_pct", 'lang', 'de'),
        # ("en_pct", 'lang', 'en'),
    ]

    # setting up the output_df
    df_index = tweets['hour'].unique()
    cols = [x[0] for x in to_calculate]
    output_df = pd.DataFrame(columns=cols, index=df_index)

    # group all tweets that appeared in the same hour and calculate stats for them
    tweets_by_hour = tweets.groupby('hour')
    for name, group in tweets_by_hour:
        total_length = len(group)
        # TODO we are ignoring total tweet count for now!
        # output_df.at[name, 'total_tweets'] = total_length
        # Normalizing: This method only works for data that has only positive values
        # output_df['total_tweets'] = output_df['total_tweets']/output_df['total_tweets'].max()
        for col, var_name, value in to_calculate:
            if col == 'total_tweets':
                pass
            else:
                output_df.at[name, col] = group[var_name].value_counts()[value] / total_length

    # sort the output by time (hour)
    output_df.sort_index(inplace=True)

    # ready to calculate the breakpoints!
    # transform data in structure that works with the ruptures package
    signal = output_df.to_numpy()
    print(f'Calculating breakpoints for signal of shape {signal.shape}')

    # Specify the model params
    algo = rpt.Pelt(model='rbf').fit(signal)
    # TODO tweak penalty
    result = algo.predict(pen=3)

    # result from ruptures typically contains one breakpoint all the way after the dataset, e.g. [205, 480] for a
    # TODO CHECK WHETHER THE BREAKPOINT IS TO BE INCLUDED OR EXCLUDED!!!
    bkp_in_data = [x-1 for x in result]
    print(bkp_in_data)
    return [x.to_pydatetime() for x in output_df.index[bkp_in_data]]


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

    # Get Breakpoints
    breakpoints = get_breakpoints(input_tweets)
    print(breakpoints)
    # last breakpoints seems to be irrelevant as it is just the last tweet
    breakpoints.pop()


    # Segmenting the original data into dfs

    # Add one breakpoints all the way in the beginning and one all the way in the end
    breakpoints_wrapped = [input_tweets['hour'].min().to_pydatetime()] + \
                          breakpoints + [input_tweets['hour'].max().to_pydatetime()]


    phases_dfs = [select_time_range(input_tweets, breakpoints_wrapped[i], breakpoints_wrapped[i+1], 'hour')
                   for i in range(len(breakpoints_wrapped) - 1)]

    # Plotting!
    pie_plot_multiplot(phases_dfs, attributes_to_plot=['tweet_type', 'user_type', 'lang']).show()
