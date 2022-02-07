from src.db.connection import connect_to_mongo
from src.db.thesis_queries import load_hypothesis_dataset, load_firestorms_individually
from src.hypotheses_testing.helpers import formula_generator, prepare_data
import statsmodels.formula.api as smf
import statsmodels.api as sm


def individual_results_to_txt(output_filename: str):
    individual_firestorms = load_firestorms_individually()

    with open(output_filename, "w") as f:
        for fs_name, firestorm_df in individual_firestorms.items():
            firestorm_dummified = prepare_data(firestorm_df)
            model = smf.glm(
                formula=formula_generator(
                    "aggression_num", firestorm_dummified.columns
                ),
                family=sm.families.Binomial(link=sm.genmod.families.links.logit),
                data=firestorm_dummified,
            ).fit()

            f.write(f"\n\n\n\n*****{fs_name}*****\n\n\n\n")
            f.write(str(model.summary()))


if __name__ == "__main__":

    connect_to_mongo()

    tweets_df = load_hypothesis_dataset()

    tweets_dummified = prepare_data(tweets_df)

    print("====LOG REG====")
    model = smf.glm(
        formula=formula_generator("aggression_num", tweets_dummified.columns),
        family=sm.families.Binomial(link=sm.genmod.families.links.logit),
        data=tweets_dummified,
    ).fit()

    print(model.summary())

    model2 = smf.logit(
        formula_generator("aggression_num", tweets_dummified.columns),
        data=tweets_dummified,
    )
    results = model2.fit()
    print(results.summary())
