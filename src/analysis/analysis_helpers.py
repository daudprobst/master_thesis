from textblob_de import TextBlobDE as TextBlob
from typing import Tuple


def contains_url(tweet: dict) -> bool:
    """Returns whether the tweet contains an url"""
    return 'urls' in tweet['entities']


def tweet_sentiment(input_text: str) -> float:
    return TextBlob(input_text).sentiment.polarity


def tweet_sentiment_category(input_text: str, cutoff: Tuple[float, float] = (-0.3, 0.3)) -> str:
    """Wrapper for tweet_sentiment that returns a categorization instead of sentiment score

    :param input_text: text for which the sentiment should be determined
    :param cutoff: cutoff points at which a tweet is determined to be negative, neutral or positive
    (e.g. with default settings Tweets with a sentiment score between -1 and -0.3 are determined to be negative,
    tweets with a sentiment between -0.3 and 0.3 as neutral and > 0.3 as positive)
    :return: Senitment of tweet as 'positive', 'neutral' or 'negative'
    """
    sentiment_score = tweet_sentiment(input_text)
    if(sentiment_score < cutoff[0]):
        return 'negative'
    if(sentiment_score < cutoff[1]):
        return 'neutral'
    return 'positive'


def tweet_type(tweet: dict) -> str:
    """ Returns the type of the tweet

    :param tweet: Full tweet object
    :return: Type of tweet ('retweet with comment', 'retweet without comment', 'reply', or 'original tweet)
    """

    if not tweet['referenced_tweets']:
        return 'original tweet'

    # TODO in which cases can there be multiple referenced tweets? For now we just look at the first reference!
    reference_type = tweet['referenced_tweets'][0]['type']

    if reference_type == 'quoted':
        return 'retweet with comment'
    elif reference_type == 'retweeted':
        return "retweet without comment"
    elif reference_type == 'replied_to':
        return "reply"
    else:  # somehow there is an unknown reference type
        raise Exception(f"Unknown tweet type with reference type {reference_type} occurred")


def user_type(row, hype_group, active_group, laggard_group):
    if row['author_id'] in hype_group:
        return "hyper-active"
    elif row['author_id'] in active_group:
        return "active"
    elif row['author_id'] in laggard_group:
        return 'laggard'
    else:
        raise Exception(f'Tweet from author {row["author_id"]} cannot be matched to any user group because this author is not in any usage group')