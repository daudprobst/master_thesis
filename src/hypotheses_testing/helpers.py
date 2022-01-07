import pandas as pd
from mongoengine.queryset.visitor import Q

from src.db.queried import QUERIES, query_iterator
from src.db.helpers import query_set_to_df
from src.db.schemes import Tweets
from scipy.stats import chi2_contingency
from typing import Sequence


def load_hypothesis_dataset(
    attributes: list[str] = ["is_offensive", "tweet_type", "user_type"]
) -> pd.DataFrame:
    tweets_collection = []
    for _, query_dict in query_iterator(QUERIES):
        query = query_dict["query"]
        firestorm_tweets_query_set = Tweets.objects(
            Q(search_params__query=query) & Q(lang="de")
        ).only(*attributes)

        firestorm_df = query_set_to_df(firestorm_tweets_query_set)

        tweets_collection.append(firestorm_df)

    return pd.concat(tweets_collection)


def dummify_categorical(
    firestorm_df: pd.DataFrame,
    variable_name: str,
    value_to_drop: str,
) -> pd.DataFrame:
    # create dummies
    dummies = pd.get_dummies(firestorm_df[variable_name])

    # replace whacko variable names
    dummies.columns = dummies.columns.str.replace(" ", "_")
    dummies.columns = dummies.columns.str.replace("-", "_")

    # drop one dummy var (base category)
    if value_to_drop:
        dummies = dummies.drop(value_to_drop, axis=1)

    firestorm_dummies = pd.concat(
        [firestorm_df, dummies],
        axis=1,
    )

    # drop old variable that is now represented through dummies
    firestorm_dummies = firestorm_dummies.drop(variable_name, axis=1)

    return firestorm_dummies


def print_chi2_contingency(contingeny_table: pd.DataFrame) -> None:
    print("==CONTINGENCY TABLE==")
    print(contingeny_table)
    stat, p, dof, expected = chi2_contingency(contingeny_table)

    print("CHI_SQUARED:")
    print(f"stat: {stat}:")
    print(f"p-value: {p}:")
    print(f"degrees of freedom: {dof}")
    print(f"expected: {expected}")


def formula_generator(y_var: str, column_names: Sequence[str]) -> None:
    columns_without_y = [col_name for col_name in column_names if col_name != y_var]
    return f'{y_var} ~ {" + ".join(columns_without_y)}'
