import pandas as pd
import plotly.express as px

def pie_plot(data: pd.DataFrame, factor_var: str, **kwargs)-> any:

    counts_by_factor = data.groupby([factor_var]).size().reset_index(name='count')

    print(counts_by_factor)

    return px.pie(counts_by_factor, names=factor_var, values='count', **kwargs)
