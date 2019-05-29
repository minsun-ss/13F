from pymongo import MongoClient
from datetime import datetime
import os
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import numpy as np

DB_URI = os.environ['MONGO_DB_URI']

def get_db():
    '''
    Configuration method to return db instance
    :return: returns db instance
    '''
    db = MongoClient(DB_URI, maxPoolSize=50, wtimeout=2500)['13f']
    return db

db = get_db()

# get query to pick
time_now = datetime.now().strftime('%Y-%m-%d')
response = db.securities.find(
    {'reportDate': '2019-03-31'},
    {'companyCIK': 1, 'companyName': 1, 'cusip': 1, 'nameOfIssuer': 1, 'value': 1, '_id': 0}
)

df = pd.DataFrame(list(response))
companies = df.groupby('companyName')['value'].sum().sort_values(ascending=False).reset_index()

'''
DASH VISUAL
'''
app = dash.Dash(__name__)
app.title = '13F filings'
server = app.server


def build_company_name_dropdown():
    '''
    Builds company list dropdown. Returns a label list of companies for the company_name_dropdown dropdown
    :return:
    '''
    dropdown = []
    for i in companies['companyName'].values:
        dropdown.append({'label': i, 'value': i})
    return dropdown

def serve_layout():
    return html.Div(children=[
        dcc.Dropdown(id='company_name_dropdown',
                     options=build_company_name_dropdown(),
                     placeholder='Total'
                     ),

        html.Div(children=[
            html.Div([
                dcc.Graph(id='companies',
                          figure=go.Bar(
                              y=companies['value'].values,
                              x=companies['companyName'].values,
                          ),)
            ], className='one-half column'),
            html.Div([
            dcc.Graph(id='securities')
            ], className='one-half column'),
        ], className='row'),
    ])
app.layout = serve_layout

@app.callback(
    [Output('companies', 'figure')],
    [Input('company_name_dropdown', 'value')]
)
def update_companies(selected):
    print(selected)

    # color in selected dropdown - don't if nothing
    if selected is None:
        companies['color'] = 'rgba(204,204,204,1)'
    else:
        companies['color'] = np.where((companies['companyName'].str.contains(selected)),
                                  'rgba(222,45,38,0.8)', 'rgba(204,204,204,1)')

    bar_figure=[{
            'data': [
                go.Bar(
                    x=companies['value'].values,
                    y=companies['companyName'].values,
                    marker=dict(color=companies['color'].values),
                    orientation='h'
                ),
            ],
            'layout':
                go.Layout(
                    margin=go.layout.Margin(
                        l=300,
                    ),
                ),
        }]

    return bar_figure

if __name__ == '__main__':
    app.run_server(debug=True)