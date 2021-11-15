import re
from typing import Sequence


def remove_leading_hashtag(hashtags: Sequence[str]) -> Sequence[str]:
    """Removes the leading hashtags symbol (if exists) for any hashtags in the list

    :param hashtags: hashtags to remove hashtag symbol for
    :return: list of hashtags without hashtag symbol
    """

    return [re.sub("#", "", hashtag, 1) for hashtag in hashtags]
