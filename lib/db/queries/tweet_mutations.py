from lib.db.schemes import Tweets

# TODO we really don't want to check each attribute before setting it...
def add_attribute_to_tweet(tweet: Tweets, attribute: str, value) -> None:
    ''' Sets an attribute for a specific tweet to a value
    :param tweet: tweet to update
    :param attribute: attribute to update for this tweet
    :param value: value that this attribute should be set to
    '''
    if attribute == 'tweet_type':
        return tweet.update(set__tweet_type=value)
    elif attribute == 'contains_url':
        return tweet.update(set__contains_url=value)
    elif attribute == 'user_type':
        return tweet.update(set__user_type=value)
    else:
        raise ValueError(f'No know preprocessing operation exists for adding the attribute {attribute}')
