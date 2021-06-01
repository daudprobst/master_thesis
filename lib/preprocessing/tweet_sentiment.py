from textblob_de import TextBlobDE as TextBlob
from typing import Tuple


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
    if sentiment_score < cutoff[0]:
        return 'negative'
    if sentiment_score < cutoff[1]:
        return 'neutral'
    return 'positive'
