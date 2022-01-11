import pandas as pd
import pytz

from src.db.connection import connect_to_mongo
from src.hypotheses_testing.helpers import (
    formula_generator,
    load_hypothesis_dataset,
    dummify_categorical,
    print_chi2_contingency,
)

from datetime import datetime
from src.utils.datetime_helpers import (
    round_to_hour,
    round_to_hour_slots,
    unix_ms_to_utc_date,
)


from sklearn.preprocessing import LabelEncoder
import statsmodels.formula.api as smf
import statsmodels.api as sm

connect_to_mongo()

tweets_df = load_hypothesis_dataset(["is_offensive", "created_at"])

tweets_df["created_at"] = tweets_df["created_at"].apply(
    lambda x: unix_ms_to_utc_date(x["$date"])
)
ger_tz = pytz.timezone("Europe/Berlin")
tweets_df["created_at"] = tweets_df["created_at"].apply(
    lambda x: x.astimezone(ger_tz)
)
tweets_df["hour"] = tweets_df["created_at"].apply(lambda x: round_to_hour(x))
tweets_df["six_hour_slot"] = tweets_df["created_at"].apply(
    lambda x: round_to_hour_slots(x)
)

tweets_df["time_of_day"] = tweets_df['six_hour_slot'].apply(lambda x: x.time)

tweets_df = tweets_df.drop(['hour', 'six_hour_slot', 'created_at'], axis=1)
contingency_table = pd.crosstab(tweets_df["time_of_day"], tweets_df['is_offensive'])
print_chi2_contingency(contingency_table)


# drop unneeded cols
tweets_df = tweets_df.drop("_id", axis=1)


# === LOG REG
# Prepare Aggression
aggr_enc = LabelEncoder()
tweets_df["aggression_num"] = aggr_enc.fit_transform(tweets_df["is_offensive"])
tweets_df = tweets_df.drop("is_offensive", axis=1)

# Prepare categoricals
firestorm_dummies = dummify_categorical(
    tweets_df, "time_of_day", datetime.strptime('18:00:00', "%H:%M:%S").time()
)

# cast col names to string
firestorm_dummies.columns = firestorm_dummies.columns.map(str)
firestorm_dummies = firestorm_dummies.rename(columns={"00:00:00": "a", "06:00:00": "b", "12:00:00": "c"})


model = smf.glm(
    formula=formula_generator("aggression_num", firestorm_dummies.columns),
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=firestorm_dummies,
).fit()
