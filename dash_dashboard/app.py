from dash import Dash, html, dcc
import pandas as pd
import analysis
import plotly.express as px


OMIT_ISSUE_TYPES = ('Epic')
FILTER_ISSUES_UNTIL = '2023-06-02'
FILTER_ISSUES_SINCE = '2023-04-15'
EXCLUDE_WEEKENDS = False
DATA_FILE = '../jira_output_data.csv'
STATUS_ORDER = ['Selected for Development', 'In Progress', 'In Review', 'Done']

data, dupes, filtered = analysis.read_data(DATA_FILE, since=FILTER_ISSUES_SINCE, until=FILTER_ISSUES_UNTIL)

issue_data, (categories, *extra) = analysis.process_issue_data(data, exclude_weekends=EXCLUDE_WEEKENDS)


# Cycle Time
def create_cycle_time_figure(df):
    cycle_data = analysis.process_cycle_data(df)
    cycle_data = cycle_data.reset_index()
    melted_data = pd.melt(cycle_data[['Work Item',
                                      'Cycle Time',
                                      'Moving Average (10 items)',
                                      'Average']],
                          ['Work Item'])
    figure = px.line(melted_data,
                     x="Work Item",
                     y="value",
                     color="variable",
                     title=f"Cycle Time Since {FILTER_ISSUES_SINCE}",
                     labels={"Work Item": "Timeline", "value": "Days"})
    tick_values = list(range(0, len(cycle_data['Work Item']), len(cycle_data['Work Item'])//10))
    tick_texts = pd.to_datetime(cycle_data['Complete Date'].values[tick_values]).strftime('%d %b')
    figure.update_layout(xaxis=dict(tickmode='array',
                                    tickvals=tick_values,
                                    ticktext=tick_texts))
    figure.add_annotation(x=cycle_data['Work Item'].max(),
                          y=cycle_data['Average'].max(),
                          text="Average = {:.2f} days".format(cycle_data['Average'].max()),
                          showarrow=False,
                          yshift=10)
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    return figure


app = Dash(__name__)

app.layout = html.Div([
    html.H1(children="Flow Metrics"),
    html.H2(children="Cycle Time Run Chart"),
    dcc.Graph(figure=create_cycle_time_figure(issue_data)),
    html.H2(children="Cycle Time Histogram"),
])

if __name__ == '__main__':
    app.run(port=8087, debug=True)
