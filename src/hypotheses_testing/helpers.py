from datetime import datetime
from typing import Sequence

import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.preprocessing import LabelEncoder
from src.utils.datetime_helpers import (
    round_to_hour_slots,
    unix_ms_to_ger_date,
)
from src.db.queried import QUERIES


def prepare_data(tweets_df: pd.DataFrame) -> pd.DataFrame:
    # drop id col
    tweets_df = tweets_df.drop("_id", axis=1)

    # calc time of day
    tweets_df["created_at"] = tweets_df["created_at"].apply(
        lambda x: unix_ms_to_ger_date(x["$date"])
    )

    tweets_df["six_hour_slot"] = tweets_df["created_at"].apply(
        lambda x: round_to_hour_slots(x, 6)
    )

    tweets_df["time_of_day"] = tweets_df["six_hour_slot"].apply(lambda x: x.time)

    tweets_df["activity_epoch"] = rowwise_activity_epoch(tweets_df)

    # Drop unnecessary vars
    tweets_df.drop(
        ["created_at", "firestorm_name", "six_hour_slot"], axis=1, inplace=True
    )

    # time of day to categorical
    tweets_dummified = dummify_categorical(
        tweets_df, "time_of_day", datetime.strptime("18:00:00", "%H:%M:%S").time()
    )

    # cast col names to string
    tweets_dummified.columns = tweets_dummified.columns.map(str)
    tweets_dummified = tweets_dummified.rename(
        columns={
            "00:00:00": "midnight_to_six_am",
            "06:00:00": "six_am_to_noon",
            "12:00:00": "noon_to_six_pm",
        }
    )

    # activity_epoch dummify
    tweets_dummified = dummify_categorical(tweets_dummified, "activity_epoch", "peak")

    # Prepare tweet type dummy vars
    tweets_dummified = dummify_categorical(
        tweets_dummified, "tweet_type", "retweet_without_comment"
    )

    tweets_dummified = dummify_categorical(tweets_dummified, "user_type", "active")

    # Prepare Aggression
    aggr_enc = LabelEncoder()
    tweets_dummified["aggression_num"] = aggr_enc.fit_transform(
        tweets_dummified["is_offensive"]
    )
    tweets_dummified = tweets_dummified.drop("is_offensive", axis=1)

    return tweets_dummified


def determine_time_epoch_from_queries_dict(
    entry_date: datetime, firestorm_name: str, queries_dict: dict = QUERIES
):
    start_date = queries_dict[firestorm_name]["true_start_date"]
    end_date = queries_dict[firestorm_name]["true_end_date"]
    return determine_time_epoch(entry_date, start_date, end_date)


def determine_time_epoch(
    entry_date: datetime, start_date: datetime, end_date: datetime
) -> str:
    if entry_date < start_date:
        return "before"
    elif entry_date >= end_date:
        return "after"
    else:
        return "peak"


def rowwise_activity_epoch(tweets_df: pd.DataFrame) -> pd.Series:
    return tweets_df.apply(
        lambda row: determine_time_epoch_from_queries_dict(
            row.created_at, row.firestorm_name
        ),
        axis=1,
    )


def dummify_categorical(
    tweets_df: pd.DataFrame,
    variable_name: str,
    value_to_drop: str,
) -> pd.DataFrame:
    # create dummies
    dummies = pd.get_dummies(tweets_df[variable_name])

    # replace whacko variable names
    try:
        dummies.columns = dummies.columns.str.replace(" ", "_")
        dummies.columns = dummies.columns.str.replace("-", "_")
    except Exception:
        print("Warning: Colnames could not be replace")

    # drop one dummy var (base category)
    if value_to_drop:
        dummies = dummies.drop(value_to_drop, axis=1)

    firestorm_dummies = pd.concat(
        [tweets_df, dummies],
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
