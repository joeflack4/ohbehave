"""Application Source Code

Comments from boilerplate author
# run following in command
# gunicorn graph:app.server -b :8000

# Resources
See: data/google_sheets.py (docstring)
"""
# ------------------------------ Import Libraries -----------------------------
from dash import dash, dash_table, html
# from dash import dash, dash_table, html, dcc
import flask
# import pandas as pd
# import plotly.graph_objs as go
from dash.dependencies import Input, Output
from pandas import DataFrame

from ohbehave.data.google_sheets import get_sheets_data
from ohbehave.data.transforms.gaming_by_modality_timeseries import transform \
    as gaming_by_modality_timeseries

# ---------------------- configuration of relative paths ----------------------
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
server = flask.Flask(__name__)  # define flask app.server
app = dash.Dash(
    __name__, external_stylesheets=external_stylesheets,
    server=server)  # call flask server


# Real data
my_data: DataFrame = get_sheets_data()
# TODO: transform data into correct format:
gaming_data: DataFrame = gaming_by_modality_timeseries(my_data)

# Boilerplate example
# df = pd.read_csv(
#     'https://gist.githubusercontent.com/chriddyp/' +
#     '5d1ea79569ed194d432e56108a04d188/raw/' +
#     'a9f9e8076b837d541398e999dcbac2b2826a81f8/' +
#     'gdp-life-exp-2007.csv')
app.layout = html.Div([
    # dcc.Graph(
    #     id='life-exp-vs-gdp',
    #     figure={
    #         'data': [
    #             go.Scatter(
    #                 x=df[df['continent'] == i]['gdp per capita'],
    #                 y=df[df['continent'] == i]['life expectancy'],
    #                 text=df[df['continent'] == i]['country'],
    #                 mode='markers',
    #                 opacity=0.7,
    #                 marker={
    #                     'size': 15,
    #                     'line': {'width': 0.5, 'color': 'white'}
    #                 },
    #                 name=i
    #             ) for i in df.continent.unique()
    #         ],
    #         'layout': go.Layout(
    #             xaxis={'type': 'log', 'title': 'GDP Per Capita'},
    #             yaxis={'title': 'Life Expectancy'},
    #             margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
    #             legend={'x': 0, 'y': 1},
    #             hovermode='closest'
    #         )
    #     }
    # ),
    # html.H6("Change the value in the text box to see callbacks in action!"),
    # html.Div(["Input: ",
    #           dcc.Input(id='my-input', value='initial value', type='text')]),
    # html.Br(),
    # html.Div(id='my-output'),
    # TODO: Get my data correct here:
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in my_data.columns],
        data=my_data.to_dict('records'))
])


@app.callback(
    Output(component_id='my-output', component_property='children'),
    Input(component_id='my-input', component_property='value')
)
def update_output_div(input_value):
    """Returning user input"""
    return '{}'.format(input_value)

