from datetime import datetime
from typing import Sequence

import pandas as pd
import pytz
from scipy.stats import chi2_contingency
from sklearn.preprocessing import LabelEncoder
from src.utils.datetime_helpers import (
    round_to_hour,
    round_to_hour_slots,
    unix_ms_to_utc_date,
)
from src.ts_analysis.pruning_raw_firestorm import (
    get_firestorm_wrapping_datetime,
)


def prepare_data(firestorm_df: pd.DataFrame) -> pd.DataFrame:
    # drop id col
    firestorm_df = firestorm_df.drop("_id", axis=1)

    # calc time of day
    firestorm_df["created_at"] = firestorm_df["created_at"].apply(
        lambda x: unix_ms_to_utc_date(x["$date"])
    )
    ger_tz = pytz.timezone("Europe/Berlin")
    firestorm_df["created_at"] = firestorm_df["created_at"].apply(
        lambda x: x.astimezone(ger_tz)
    )
    firestorm_df["hour"] = firestorm_df["created_at"].apply(lambda x: round_to_hour(x))
    firestorm_df["six_hour_slot"] = firestorm_df["created_at"].apply(
        lambda x: round_to_hour_slots(x)
    )

    firestorm_df["time_of_day"] = firestorm_df["six_hour_slot"].apply(lambda x: x.time)

    firestorm_df.drop(["created_at", "hour", "six_hour_slot"], axis=1, inplace=True)

    # time of day to categorical
    firestorm_dummified = dummify_categorical(
        firestorm_df, "time_of_day", datetime.strptime("18:00:00", "%H:%M:%S").time()
    )

    # cast col names to string
    firestorm_dummified.columns = firestorm_dummified.columns.map(str)
    firestorm_dummified = firestorm_dummified.rename(
        columns={
            "00:00:00": "0am to 6 am",
            "06:00:00": "6am to 12am",
            "12:00:00": "12am to 6pm",
        }
    )

    # Prepare tweet type dummy vars
    firestorm_dummified = dummify_categorical(
        firestorm_dummified, "tweet_type", "retweet_without_comment"
    )

    firestorm_dummified = dummify_categorical(
        firestorm_dummified, "user_type", "laggard"
    )

    # Prepare Aggression
    aggr_enc = LabelEncoder()
    firestorm_dummified["aggression_num"] = aggr_enc.fit_transform(
        firestorm_dummified["is_offensive"]
    )
    firestorm_dummified = firestorm_dummified.drop("is_offensive", axis=1)

    return firestorm_dummified


def determine_time_epoch(
    entry_date: datetime, start_date: datetime, end_date: datetime
) -> str:
    if entry_date < start_date:
        return "before"
    elif entry_date >= end_date:
        return "after"
    else:
        return "peak"


def add_time_epoch_column(tweets_df: pd.DataFrame) -> pd.DataFrame:
    wrapping_datetimes = get_firestorm_wrapping_datetime(tweets_df)
    tweets_df["time_epoch"] = tweets_df.apply(
        lambda row: determine_time_epoch(
            row.created_at, wrapping_datetimes[0], wrapping_datetimes[1]
        ),
        axis=1,
    )
    return tweets_df


def dummify_categorical(
    firestorm_df: pd.DataFrame,
    variable_name: str,
    value_to_drop: str,
) -> pd.DataFrame:
    # create dummies
    dummies = pd.get_dummies(firestorm_df[variable_name])

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
