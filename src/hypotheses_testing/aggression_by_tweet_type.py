import pandas as pd
from src.db.connection import connect_to_mongo
from src.hypotheses_testing.helpers import (
    formula_generator,
    load_hypothesis_dataset,
    dummify_categorical,
    print_chi2_contingency,
)

from sklearn.preprocessing import LabelEncoder
import statsmodels.formula.api as smf
import statsmodels.api as sm

connect_to_mongo()

firestorm_df = load_hypothesis_dataset(["is_offensive", "tweet_type"])

# === Correlation Analysis
firestorm_contingency_table = pd.crosstab(
    firestorm_df["is_offensive"], firestorm_df["tweet_type"]
)
print_chi2_contingency(firestorm_contingency_table)


# === LOG REG
# Prepare Aggression
aggr_enc = LabelEncoder()
firestorm_df["aggression_num"] = aggr_enc.fit_transform(firestorm_df["is_offensive"])
firestorm_df = firestorm_df.drop("is_offensive", axis=1)

# Prepare categoricals
firestorm_dummies = dummify_categorical(
    firestorm_df, "tweet_type", "retweet_without_comment"
)

# drop unneeded cols
firestorm_dummies = firestorm_dummies.drop("_id", axis=1)

print("====TWEET TYPE====")
model = smf.glm(
    formula=formula_generator("aggression_num", firestorm_dummies.columns),
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=firestorm_dummies,
).fit()

print(model.summary())


# ==== Lets check for the influence of user_type on tweet type


firestorm_df_with_user = load_hypothesis_dataset(
    ["is_offensive", "tweet_type", "user_type"]
)

# Prepare categoricals

user_tweet_type_dummies = dummify_categorical(
    firestorm_df_with_user, "tweet_type", None
)
user_tweet_type_dummies = dummify_categorical(
    user_tweet_type_dummies, "user_type", "laggard"
)

print("====TWEET TYPE====")
model2 = smf.glm(
    formula="retweet_without_comment ~ active + hyper_active",
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=user_tweet_type_dummies,
).fit()

print(model2.summary())
