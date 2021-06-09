from lib.db.queries.tweet_queries import get_tweets_for_search_query
from lib.db.connection import connect_to_mongo
from json import loads
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour
import pandas as pd
import plotly.express as px


def entries_per_hour(tweets: pd.DataFrame):
    for i,tweet in tweets.iterrows():
        tweet['created_at'] = unix_ms_to_date(tweet['created_at']["$date"])
        tweets._set_value(i, "hour", round_to_hour(tweet['created_at']))

    print(tweets.iloc[0])
    return tweets.groupby(['hour']).size().reset_index(name='count')

def rates_over_time(tweets: pd.DataFrame):
    #################
    from scipy import signal

    output_df['hour'] = output_df.index
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=output_df['hour'],
        y=signal.savgol_filter(output_df['retweet_pct'],
                               53,  # window size used for filtering
                               3),  # order of fitted polynomial
        name='retweet pct'
    ))
    fig.add_trace(go.Scatter(
        x=output_df['hour'],
        y=signal.savgol_filter(output_df['laggards_pct'],
                               53,  # window size used for filtering
                               3),  # order of fitted polynomial
        name='Laggards pct'
    ))
    fig.add_trace(go.Scatter(
        x=output_df['hour'],
        y=signal.savgol_filter(output_df['de_pct'],
                               53,  # window size used for filtering
                               3),  # order of fitted polynomial
        name='de pct'
    ))
    fig.show()

    '''
    px.line(output_df, x="hour", y="retweet_pct", title="retweet pct").show()
    px.line(output_df, x="hour", y="retweet_pct", title="retweet pct").show()
    px.line(output_df, x="hour", y="laggards_pct", title="laggards_pct").show()
    px.line(output_df, x="hour", y="de_pct", title="de_pct").show()
    '''
    output_df = output_df.drop(['hour'], axis=1)
    #################


if __name__ == "__main__":
    connect_to_mongo()

    firestorm_tweets_selection = loads(
        get_tweets_for_search_query('pinkygloves').only(
            'user_type', 'created_at', 'text', 'tweet_type', "contains_url", "lang"
        ).to_json()
    )

    firestorm_df = pd.DataFrame.from_records(firestorm_tweets_selection)

    px.line(entries_per_hour(firestorm_df), x="hour", y="count").show()

    #pie_plot(firestorm_df, 'user_type',
    #         color_discrete_sequence=[clrs['purple'], clrs['teal'], clrs['aqua']]).show()


    # px.line(firestorm_df, x="hour", y="lifeExp").show()
    '''
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'contains_url', measure_type='percentage',
                        title="Usage of URLs", color_discrete_sequence=[clrs['blue'], clrs['olive']]).show()
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'tweet_type', title="Tweet Type", measure_type='percentage',
                        color_discrete_sequence=[clrs['maroon'], clrs['fuchsia'], clrs['orange'], clrs['yellow']]).show()
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'user_type', measure_type='percentage',
                        color_discrete_sequence=[clrs['purple'], clrs['teal'], clrs['aqua']]).show()
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'lang', title="Language", measure_type='percentage',
                    ).show()
    '''

