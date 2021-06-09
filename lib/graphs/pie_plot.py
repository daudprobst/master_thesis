import pandas as pd
import plotly.express as px
from typing import Sequence
from plotly.subplots import make_subplots
import plotly.graph_objects as go


def pie_plot(data: pd.DataFrame, grouping_var: str, **kwargs) -> any:
    counts_by_factor = data.groupby([grouping_var]).size().reset_index(name='count')
    print(counts_by_factor)
    return px.pie(counts_by_factor, names=grouping_var, values='count', **kwargs)


def pie_plot_multiplot(list_of_dfs: Sequence[pd.DataFrame], attributes_to_plot: Sequence) -> go:
    """

    :param list_of_dfs: list of data for which a plot should be generated (uniform structure across dfs is necessary)
    :param attributes_to_plot: list of attributes to plot for (must all be categorical)
    :return: plotly graph object containing all the subplots (use .show() to display the plot)
    """
    NR_COLS = 3
    nr_rows_required = ((len(list_of_dfs) * len(attributes_to_plot) - 1) // NR_COLS) + 1
    specs_row = [{"type": "pie"} for i in range(NR_COLS)]
    specs_required = [specs_row for i in range(nr_rows_required)]

    fig = make_subplots(
        rows=nr_rows_required, cols=NR_COLS,
        specs=specs_required,
    )

    for phase_i, phase_df in enumerate(list_of_dfs):
        for plot_counter, attribute in enumerate(attributes_to_plot, start=phase_i * len(attributes_to_plot)):
            col = (plot_counter % NR_COLS) + 1
            row = (plot_counter // NR_COLS) + 1
            fig.add_trace(go.Pie(labels=phase_df[attribute], title=f'Phase {phase_i} analyzing {attribute}'),
                          row=row, col=col)

    return fig