

def user_type(tweet: dict, user_groups: dict) -> str:
    if tweet['author_id'] in user_groups['hyper_active_users']:
        return "hyper-active"
    elif tweet['author_id'] in user_groups["active_users"]:
        return "active"
    elif tweet['author_id'] in user_groups["lurking_users"]:
        return 'laggard'
    else:
        raise Exception(f'Tweet from author {tweet["author_id"]} cannot be matched to any user'
                        f'group because this author is not in any usage group')
