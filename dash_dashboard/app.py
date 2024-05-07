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

# Cycle Time Data
cycle_data = analysis.process_cycle_data(issue_data)
cycle_data = cycle_data.reset_index()

# Throughput Data
throughput, throughput_per_week = analysis.process_throughput_data(issue_data,
                                                                   since=FILTER_ISSUES_SINCE,
                                                                   until=FILTER_ISSUES_UNTIL)
throughput_per_week = throughput_per_week.reset_index()

# Work in Progress
wip, _ = analysis.process_wip_data(issue_data, since=FILTER_ISSUES_SINCE, until=FILTER_ISSUES_UNTIL)
wip = wip.reset_index()

age_data = analysis.process_wip_age_data(issue_data,
                                         since=FILTER_ISSUES_SINCE,
                                         until=FILTER_ISSUES_UNTIL)


def create_cycle_time_run_chart(df):
    melted_data = pd.melt(df[['Work Item',
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
    tick_values = list(range(0, len(df['Work Item']), len(df['Work Item']) // 10))
    tick_texts = pd.to_datetime(df['Complete Date'].values[tick_values]).strftime('%d %b')
    figure.update_layout(xaxis=dict(tickmode='array',
                                    tickvals=tick_values,
                                    ticktext=tick_texts))
    figure.add_annotation(x=df['Work Item'].max(),
                          y=df['Average'].max(),
                          text="Average = {:.2f} days".format(df['Average'].max()),
                          showarrow=False,
                          yshift=10)
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    return figure


def create_cycle_time_histogram(df):
    figure = px.histogram(df['Cycle Time'],
                          nbins=7,
                          title=f"Cycle Time Histogram Since {FILTER_ISSUES_SINCE}")
    figure.update_layout(xaxis_title="Days",
                         yaxis_title="Frequency")
    figure.update_layout(showlegend=False,
                         bargap=0.1)
    return figure


def create_cycle_time_scatterplot(df):
    figure = px.scatter(df,
                        x=cycle_data['Complete Date'],
                        y=cycle_data['Cycle Time'],
                        title="Cycle Time Scatterplot")
    for v in (0.25, 0.5, 0.75, 0.85, 0.95):
        figure.add_hline(y=cycle_data['Cycle Time'].quantile(v),
                         line_dash="dash",
                         line_width=1)
        figure.add_annotation(x=cycle_data['Complete Date'].max(),
                              y=cycle_data['Cycle Time'].quantile(v),
                              text="{:.2f}% {:.2f}".format(v * 100, cycle_data['Cycle Time'].quantile(v)),
                              showarrow=False,
                              yshift=10)
    figure.update_xaxes(tickformat="%e %b, %Y")
    return figure


def create_throughput_per_week_run_chart(df):
    import plotly.graph_objects as go
    figure = px.area(df,
                     x=df['Date'],
                     y=df["Throughput"],
                     title=f"Throughput per Week Since {FILTER_ISSUES_SINCE}",
                     labels={"Date": "Week"})
    figure.add_trace(go.Scatter(x=df['Date'],
                                y=df["Moving Average (4 weeks)"],
                                name="Moving Average (4w)"))
    figure.add_trace(go.Scatter(x=df['Date'],
                                y=df["Average"],
                                name="Average"))
    figure.add_annotation(x=df['Date'].min(),
                          y=df['Average'].max(),
                          text="Average = {:.2f} days".format(df['Average'].max()),
                          showarrow=False,
                          yshift=10)
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    figure.update_layout(hovermode="x unified")
    return figure


def create_throughput_histogram(df):
    figure = px.histogram(df['Throughput'],
                          nbins=7,
                          title=f"Throughput Histogram Since {FILTER_ISSUES_SINCE}")
    figure.update_layout(xaxis_title="Throughput (per week)",
                         yaxis_title="Frequency")
    figure.update_layout(showlegend=False,
                         bargap=0.1)
    return figure


def create_cfd_by_categories(df):
    import plotly.graph_objects as go
    status_categories = {'To Do': 'lightgray',
                         'In Progress': 'lightskyblue',
                         'Done': 'lightseagreen'}
    f = analysis.process_flow_category_data(df, since=FILTER_ISSUES_SINCE, until=FILTER_ISSUES_UNTIL)
    figure = go.Figure()
    for status_category in reversed(status_categories):
        figure.add_trace(go.Scatter(x=f.index.values,
                                    y=f[status_category],
                                    stackgroup="stack_group",
                                    name=status_category,
                                    line=dict(color=status_categories[status_category]),
                                    fillcolor=status_categories[status_category]))
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    figure.update_layout(hovermode="x unified")
    figure.update_layout(xaxis_title="Timeline",
                         yaxis_title="Items")
    figure.update_layout(title=f"Cumulative Flow Diagram by Status Category since {FILTER_ISSUES_SINCE}")
    return figure


def create_cfd_by_status(df):
    import plotly.graph_objects as go
    f = analysis.process_flow_data(df, since=FILTER_ISSUES_SINCE, until=FILTER_ISSUES_UNTIL)
    figure = go.Figure()
    for status in reversed(STATUS_ORDER):
        figure.add_trace(go.Scatter(x=f.index.values,
                                    y=f[status],
                                    stackgroup="stack_group",
                                    name=status))
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    figure.update_layout(hovermode="x unified")
    figure.update_layout(xaxis_title="Timeline",
                         yaxis_title="Items")
    figure.update_layout(title=f"Cumulative Flow Diagram by Status since {FILTER_ISSUES_SINCE}")
    return figure


def create_wip_run_chart(df):
    wip_melted = pd.melt(df, ['Date'])
    wip_melted['value'] = wip_melted['value'].astype(float)
    figure = px.line(wip_melted,
                     x="Date",
                     y="value",
                     color="variable",
                     title=f"Work in Progress per Day Since {df['Date'].min().strftime('%Y-%m-%d')}",
                     labels={"Date": "Timeline", "value": "Items"})
    figure.update_layout(legend=dict(yanchor="top",
                                     y=0.99,
                                     xanchor="left",
                                     x=0.01))
    return figure


def create_wip_age_chart(df):
    figure = px.scatter(df,
                        x='last_issue_status',
                        y='Age in Stage',
                        title="Current Work in Progress Aging",
                        labels={"last_issue_status": "Status", "Age in Stage": "Aging"})
    quantiles = {'P50': 0.5, 'P75': 0.75, 'P95': 0.95}
    for quantile in quantiles:
        figure.add_hline(y=df[quantile].iloc[0],
                         line_dash="dash",
                         line_width=1)
        figure.add_annotation(x=df['last_issue_status'].min(),
                              y=df[quantile].iloc[0],
                              text="{:.2f} days ({:.0f}%)".format(df[quantile].iloc[0], quantiles[quantile] * 100),
                              showarrow=False,
                              yshift=10)
    figure.update_layout(hovermode="x unified")
    return figure


app = Dash(__name__)

app.layout = html.Div([
    html.H1(children="Flow Metrics"),
    html.H2(children="Cycle Time Run Chart"),
    dcc.Graph(figure=create_cycle_time_run_chart(cycle_data)),
    html.H2(children="Cycle Time Histogram"),
    dcc.Graph(figure=create_cycle_time_histogram(cycle_data)),
    html.H2(children="Cycle Time Scatterplot"),
    dcc.Graph(figure=create_cycle_time_scatterplot(cycle_data)),
    html.H2(children="Throughput per Week"),
    dcc.Graph(figure=create_throughput_per_week_run_chart(throughput_per_week)),
    html.H2(children="Throughput Histogram"),
    dcc.Graph(figure=create_throughput_histogram(throughput_per_week)),
    html.H2(children="Cumulative Flow Diagram by Status Category"),
    dcc.Graph(figure=create_cfd_by_categories(data)),
    html.H2(children="Cumulative Flow Diagram by Status"),
    dcc.Graph(figure=create_cfd_by_status(data)),
    html.H2(children="Work In Progress"),
    dcc.Graph(figure=create_wip_run_chart(wip)),
    html.H2(children="Current WIP Aging"),
    dcc.Graph(figure=create_wip_age_chart(age_data)),
])

if __name__ == '__main__':
    app.run(port=8087, debug=True)
