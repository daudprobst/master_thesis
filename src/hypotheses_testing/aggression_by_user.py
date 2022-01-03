import pandas as pd
from src.db.connection import connect_to_mongo
from src.hypotheses_testing.helpers import load_hypothesis_dataset, dummify_categorical

from sklearn.preprocessing import LabelEncoder
import statsmodels.formula.api as smf
import statsmodels.api as sm
from scipy.stats import chi2_contingency

connect_to_mongo()

firestorm_df = load_hypothesis_dataset(["is_offensive", "user_type"])

# === Correlation Analysis
firestorm_contingency_table = pd.crosstab(
    firestorm_df["is_offensive"], firestorm_df["user_type"]
)
print(firestorm_contingency_table)
stat, p, dof, expected = chi2_contingency(firestorm_contingency_table)

print("CHI_SQUARED:")
print(f"stat: {stat}:")
print(f"p-value: {p}:")
print(f"degrees of freedom: {dof}")
print(f"expected: {expected}")


# === LOG REG
# Prepare Aggression
aggr_enc = LabelEncoder()
firestorm_df["aggression_num"] = aggr_enc.fit_transform(firestorm_df["is_offensive"])
firestorm_df = firestorm_df.drop("is_offensive", axis=1)
# Prepare categoricals
firestorm_dummies = dummify_categorical(firestorm_df, "user_type", 'laggard')

print("====USERS TYPE====")
model_users = smf.glm(
    formula="aggression_num ~ active + hyper_active",
    family=sm.families.Binomial(link=sm.genmod.families.links.logit),
    data=firestorm_dummies,
).fit()

print(model_users.summary())
