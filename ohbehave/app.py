"""OhBehave

Resources
  1. See: data/google_sheets.py (docstring)
  2. Dash docs: https://dash.plotly.com/
     - Graphs: https://dash.plotly.com/dash-core-components/graph
  3. Al usage: https://docs.google.com/spreadsheets/d/14Cjd_LuURTPEytKBLT0gAvk2GwyGMztYEvRXaZfnG2I/edit#gid=2081395842

TODO's
  1. Line graph: avg gaming/night by week (or variation)
  2. A more complex UI, so that I can tab through multiple graphs
  3. Add more graphs: (i) alcohol, (ii) sleep
  4. Add new cols to tracking: whatev useful me. Cravings. Emotions. Trigger
  5. combine graphs into one: 'data': [go.Line() for i in [...data for each graph...]]
"""
import flask
import pandas as pd
from dash import dash, dash_table, html, dcc
from dash.dependencies import Input, Output
from plotly import graph_objs as go

from ohbehave.data.transforms.data_by_date import data_by_date


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
SERVER = flask.Flask(__name__)
APP = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=SERVER)


# Real data
data_by_date: pd.DataFrame = data_by_date(use_cache=True)  # todo: use_cache: change to False for prod

# Boilerplate example
APP.layout = html.Div([
    # TODO: line graph: alcohol first. then can repurpose to gaming
    #  - Do it by date first. then add `week #` (and other cols in al report) to data_by_date()
    # https://dash.plotly.com/dash-core-components/graph
    # TODO: Aggregate by week/month (later chosen by user input)
    #  https://community.plotly.com/t/aggregating-time-series-data/15466
    #  https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html
    dcc.Graph(
        id='data-graph',
        figure={
            'data': [
                {
                    'x': data_by_date['Date'],
                    'y': data_by_date['Drinks.tot'],
                    'name': 'alcohol',
                    'connectgaps': True,
                    'opacity': 0.7,
                },
            ],
            # todo:
            'layout': go.Layout(
                # xaxis={'type': 'log', 'title': 'Date'},
                xaxis={'title': 'Date'},
                yaxis={'title': 'Drinks'},
                # margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                # legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
    ),

    # todo: repurpose: allow to summarize by day/week/month
    html.H6("Change the value in the text box to see callbacks in action!"),
    html.Div(["Input: ", dcc.Input(id='my-input', value='initial value', type='text')]),
    html.Br(),
    html.Div(id='my-output'),

    # todo: repurpose/remove whenever
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in data_by_date.columns],
        data=data_by_date.to_dict('records'))
])


@APP.callback(
    Output(component_id='my-output', component_property='children'),
    Input(component_id='my-input', component_property='value'))
def update_output_div(input_value):
    """Returning user input"""
    return '{}'.format(input_value)


if __name__ == "__main__":
    APP.server.run(host="0.0.0.0", debug=False)
