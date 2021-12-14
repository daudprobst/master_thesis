import pandas as pd
from src.db.connection import connect_to_mongo
from src.hypotheses_testing.helpers import load_hypothesis_dataset, dummify_categoricals


import statsmodels.formula.api as smf
import statsmodels.api as sm
from scipy.stats import chi2_contingency

connect_to_mongo()

firestorm_df = load_hypothesis_dataset()

print("=====AGGRESSION BY TWEET TYPE=====")

# === Correlation Analysis
firestorm_contingency_table = pd.crosstab(
    firestorm_df["is_offensive"], firestorm_df["tweet_type"]
)
print(firestorm_contingency_table)
stat, p, dof, expected = chi2_contingency(firestorm_contingency_table)

print("CHI_SQUARED:")
print(f"stat: {stat}:")
print(f"p-value: {p}:")
print(f"degrees of freedom: {dof}")
print(f"expected: {expected}")


# === LOG REG
firestorm_dummies = dummify_categoricals(firestorm_df)

print("====TWEET TYPE====")
model_tweet_type = smf.glm(
    formula="aggression_num ~ original_tweet + reply + retweet_with_comment",
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=firestorm_dummies,
).fit()

print(model_tweet_type.summary())

print("====USERS TYPE====")
model_users = smf.glm(
    formula="aggression_num ~ active + hyper_active",
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=firestorm_dummies,
).fit()

print(model_users.summary())
