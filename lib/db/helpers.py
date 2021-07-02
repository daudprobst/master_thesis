import pandas as pd
import json
from mongoengine import QuerySet


def query_set_to_df(input_data: QuerySet) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        json.loads(input_data.to_json())
    )