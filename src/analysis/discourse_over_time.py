from lib.db.queries.tweet_queries import get_tweets_for_hashtags
from lib.db.connection import connect_to_mongo
from json import loads
from lib.utils.datetime_helpers import unix_ms_to_date, round_to_hour, round_to_hour_slots
import pandas as pd
from src.graphs.bar_plot import percentage_bar_plot_over_time
from src.graphs.pie_plot import pie_plot
from src.analysis.analysis_helpers import contains_url, tweet_sentiment_category, tweet_type, user_type
from lib.utils.constants import clrs

if __name__ == "__main__":
    connect_to_mongo()

    firestorm_tweets_selection = loads(
        get_tweets_for_hashtags('#Antisemitismus', 'Gelsenkirchen').only(
            'author_id', 'created_at', 'text', 'referenced_tweets', "entities__urls"
        ).to_json()
    )

    #preprocessing
    for i, entry in enumerate(firestorm_tweets_selection):
        entry['created_at'] =  unix_ms_to_date(entry['created_at']["$date"])
        entry['hour'] =  round_to_hour(entry['created_at'])
        entry['hour_slot'] = round_to_hour_slots(entry['created_at'], 6)
        entry['tweet_type'] = tweet_type(entry)
        entry['contains_url'] = contains_url(entry)
        #entry['sentiment_category'] = tweet_sentiment_category(entry['text'])
        #if i%100 == 0:
        #    print(f'Calculated attributes for {i} tweets')

    firestorm_df = pd.DataFrame.from_records(firestorm_tweets_selection)

    #== add user group to data frame
    firestorms_user_activity_counts = firestorm_df.groupby(['author_id']).size().reset_index(name='count')

    firestorms_user_activity_counts.sort_values(by=["count"], ascending=False, inplace=True)

    hyper_active_users = list(
        firestorms_user_activity_counts.iloc[:len(firestorms_user_activity_counts) // 100]['author_id']
    )
    active_users = list(
        firestorms_user_activity_counts.iloc[
            len(firestorms_user_activity_counts) // 100: len(firestorms_user_activity_counts) // 10]['author_id'])
    lurking_users = list(firestorms_user_activity_counts.iloc[len(firestorms_user_activity_counts) // 10:]['author_id'])

    print(f'{len(hyper_active_users)} hyper_active_users, {len(active_users)}'
          f' active_users and {len(lurking_users)} lurking_users')

    firestorm_df['user_group'] = firestorm_df.apply(
        lambda row: user_type(row, hyper_active_users, active_users, lurking_users), axis=1)

    # ====

    # Start Grouping for Analysis

    pie_plot(firestorm_df, 'user_group',
             color_discrete_sequence=[clrs['purple'], clrs['teal'], clrs['aqua']]).show()


    '''
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'contains_url', measure_type='percentage',
                        title="Usage of URLs", color_discrete_sequence=[clrs['blue'], clrs['olive']]).show()
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'tweet_type', title="Tweet Type", measure_type='percentage',
                        color_discrete_sequence=[clrs['maroon'], clrs['fuchsia'], clrs['orange'], clrs['yellow']]).show()
    percentage_bar_plot_over_time(firestorm_df, None, 'hour_slot', 'user_group', measure_type='percentage',
                        color_discrete_sequence=[clrs['purple'], clrs['teal'], clrs['aqua']]).show()
    '''