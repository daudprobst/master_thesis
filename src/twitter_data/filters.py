def equality_filter_factory(attribute: str, expected_value: any):
    def equality_filter(tweets):
        return tweets[tweets[attribute] == expected_value]

    return equality_filter


def between_filter_factory(attribute: str, lower_bound: any, upper_bound: any):
    def between_filter(tweets):
        return tweets[
            (tweets[attribute] >= lower_bound) & (tweets[attribute] < upper_bound)
        ]

    return between_filter


def default_filters_factory(query_dict: dict):
    return [
        between_filter_factory(
            "created_at", query_dict["true_start_date"], query_dict["true_end_date"]
        ),
        equality_filter_factory("lang", "de"),
    ]
