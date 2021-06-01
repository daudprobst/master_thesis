
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
