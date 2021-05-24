import pandas as pd
import plotly.express as px

def percentage_bar_plot(data: pd.DataFrame, plot, grouping_var: str,
                            factor_var: str, min_entry_count: int = 500) -> any:

    group_counts_by_factor = data.groupby([grouping_var, factor_var]).size().reset_index(name='level_count')
    print(group_counts_by_factor)

    # Overall tweets in a specific time segment/hour slot
    group_sizes = data.groupby(grouping_var).size()

    # DROPPING TIME SLOT WITH LESS THAN 500 TWEETS
    low_count_groups = []
    for hour, count in zip(group_sizes.index, group_sizes):
        if (count < min_entry_count):
            low_count_groups.append(hour)

    print(f'Tweets for this time slot should be removed {low_count_groups}')
    firestorm_counts = group_counts_by_factor[~group_counts_by_factor[grouping_var].isin(low_count_groups)]

    firestorm_counts['level_percentage'] = firestorm_counts.apply(
        lambda row: row['level_count'] / group_sizes[row[grouping_var]], axis=1)

    return px.bar(firestorm_counts, x=grouping_var, y="level_percentage", color=factor_var,
                 title="Untitled")
