import pandas as pd
from src.db.connection import connect_to_mongo
from src.db.thesis_queries import load_firestorms_individually, load_hypothesis_dataset
from src.hypotheses_testing.helpers import formula_generator, prepare_data
import statsmodels.formula.api as smf
import statsmodels.api as sm


if __name__ == "__main__":

    connect_to_mongo()

    firestorm_df = load_hypothesis_dataset(
        [
            "is_offensive",
            "user_type",
            "tweet_type",
            "created_at",
        ]
    )

    firestorm_dummified = prepare_data(firestorm_df)

    print("====LOG REG====")
    model_users = smf.glm(
        formula=formula_generator("aggression_num", firestorm_dummified.columns),
        family=sm.families.Binomial(link=sm.genmod.families.links.logit),
        data=firestorm_dummified,
    ).fit()

    print(model_users.summary())
