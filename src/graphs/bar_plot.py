import pandas as pd
import plotly.express as px


def percentage_bar_plot_over_time(
    data: pd.DataFrame,
    grouping_var: str,
    factor_var: str,
    measure_type: str = "count",
    min_entry_count: int = 50,
    **kwargs
) -> any:

    group_counts_by_factor = (
        data.groupby([grouping_var, factor_var]).size().reset_index(name="count")
    )

    # Overall tweets in a specific time segment/hour slot
    group_sizes = data.groupby(grouping_var).size()

    # DROPPING TIME SLOT WITH LESS THAN 500 TWEETS
    low_count_groups = []
    for hour, count in zip(group_sizes.index, group_sizes):
        if count < min_entry_count:
            low_count_groups.append(hour)

    group_counts_by_factor = group_counts_by_factor[
        ~group_counts_by_factor[grouping_var].isin(low_count_groups)
    ]

    group_counts_by_factor.loc[:, "percentage"] = group_counts_by_factor.apply(
        lambda row: row["count"] / group_sizes[row[grouping_var]], axis=1
    )

    return px.bar(
        group_counts_by_factor,
        x=grouping_var,
        y=measure_type,
        hover_data=["count", "percentage"],
        color=factor_var,
        **kwargs
    )
