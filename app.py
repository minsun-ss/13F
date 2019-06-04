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
    {},
    {'companyCIK': 1, 'companyName': 1, 'cusip': 1, 'nameOfIssuer': 1, 'value': 1, '_id': 0}
)

# build out the dataframes used for the visualization
df = pd.DataFrame(list(response))
companies = df.groupby('companyName')['value'].sum().sort_values(ascending=True).reset_index()
securities = df.groupby('nameOfIssuer')['value'].sum().sort_values(ascending=True).reset_index()
securities = securities[~(securities.value==0)]
security_by_company = df.groupby(['companyName','nameOfIssuer'])['value'].sum().sort_values(ascending=True).reset_index()
security_by_company = security_by_company[~(security_by_company.value == 0)]


'''
===========
DASH VISUAL
===========
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

def build_company_figure(selected):
    '''
    Builds the data figure for the company horizontal bar chart. Also highlights specific commpanies (if selected
    by dropdown) by color to indicate what is loaded in the securities chart.
    :param selected:
    :return: data figure for company horizontal bar chart
    '''
    # color in selected dropdown - don't if nothing
    if selected is None:
        companies['color'] = 'rgba(204,204,204,1)'
    else:
        companies['color'] = np.where((companies['companyName'].str.contains(selected)),
                                  'rgba(222,45,38,0.8)', 'rgba(204,204,204,1)')
    company_figure={
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
                    height=700,
                    margin=go.layout.Margin(
                        l=300,
                    ),
                ),
        }

    return company_figure

def build_security_figure(selected):
    '''
    Builds securities holdings of a specific company and sorts them in value order. Does not show securities with
    0 value. As it is listed by issuer, it does not differentiate on type (e.g., bond, equity, option, etc). If no
    company was selected in the dropdown, will display the holdings of the entire set.

    :param selected: Selected dropdown item
    :return: data figure of securities holdings
    '''
    if selected is None:
        # dynamically resize chart plus add padding for small charts
        chart_height = (len(securities) * 20) + 200
        security_figure  = {
            'data': [
                go.Bar(
                    x=securities['value'].values,
                    y=securities['nameOfIssuer'].values,
                    orientation='h'
                ),
            ],
            'layout':
                go.Layout(
                    height=chart_height,
                    margin=go.layout.Margin(
                        l=300,
                    ),
                    yaxis=dict(
                        hoverformat='$.2f'
                    )
                ),
        }
    else:
        tempdf = (security_by_company[security_by_company['companyName'].str.
                  contains(selected)].sort_values(by='value', ascending=True))

        # dynamically resize the chart plus add padding for very small charts
        chart_height = (len(tempdf) * 20) + 200
        security_figure  = {
            'data': [
                go.Bar(
                    x=tempdf['value'].values,
                    y=tempdf['nameOfIssuer'].values,
                    orientation='h'
                ),
            ],
            'layout':
                go.Layout(
                    height=chart_height,
                    margin=go.layout.Margin(
                        l=300,
                    ),
                    yaxis=dict(
                        hoverformat='$.2f'
                    )
                ),
        }

    return security_figure


def serve_layout():
    return html.Div(children=[
        html.H5(children='13F-H Securities Holdings, Last 7 Days of Filings - see http://www.stuffofminsun.com'),
        html.Div(children=[
            html.Div(children=[
                dcc.Dropdown(id='company_name_dropdown',
                             options=build_company_name_dropdown(),
                             placeholder='Total'
                             ),
            ], className='one-half column'),
        ], className='row'),
        html.Div(children=[
            html.Div([
                dcc.Graph(id='companies',
                          figure=go.Bar(
                              y=companies['value'].values,
                              x=companies['companyName'].values,
                          ),)
            ], className='one-half column'),
            html.Div([
                dcc.Graph(id='securities', style={'height': 700, 'overflowY': 'scroll', 'overflowX': 'hidden'})
            ], className='one-half column'),
        ], className='row'),
    ])
app.layout = serve_layout

@app.callback(
    [Output('companies', 'figure'),
     Output('securities', 'figure')],
    [Input('company_name_dropdown', 'value')]
)
def update_companies(selected):
    return build_company_figure(selected), build_security_figure(selected)

if __name__ == '__main__':
    app.run_server(debug=True)