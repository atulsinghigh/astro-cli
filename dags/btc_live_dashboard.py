import pandas as pd
import plotly.graph_objs as go
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import os

# Path to your Airflow-generated CSV
CSV_PATH = "/usr/local/airflow/files/btc_price_inr.csv"

# Create Dash app
app = Dash(__name__)
app.title = "Live BTC Price Dashboard 💸"

# Dashboard Layout
app.layout = html.Div(style={'fontFamily': 'Arial', 'padding': '30px'}, children=[
    html.H1("🪙 Live Bitcoin Price in INR", style={'textAlign': 'center'}),
    
    dcc.Graph(id='btc-graph'),

    dcc.Interval(
        id='interval-refresh',
        interval=60*1000,  # refresh every 1 minute
        n_intervals=0
    ),

    html.Div("🔄 Updates every 60 seconds", style={'textAlign': 'center', 'color': 'gray'})
])

# Callback to update graph
@app.callback(
    Output('btc-graph', 'figure'),
    Input('interval-refresh', 'n_intervals')
)
def update_graph(n):
    if not os.path.exists(CSV_PATH):
        return go.Figure()

    df = pd.read_csv(CSV_PATH)
    if df.empty:
        return go.Figure()

    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['Timestamp'],
        y=df['Price (INR)'],
        mode='lines+markers',
        name='BTC in INR',
        line=dict(color='royalblue', width=2),
        marker=dict(size=5),
        hovertemplate=
            '<b>Time</b>: %{x}<br>'+
            '<b>INR</b>: ₹%{y:,.2f}<br>'+
            '<b>USD</b>: $%{customdata[0]:,.2f}<br>'+
            '<b>Rate</b>: %{customdata[1]:.2f}',
        customdata=df[['Price (USD)', 'Rate (USD→INR)']].values
    ))

    fig.update_layout(
        xaxis_title='Time (IST)',
        yaxis_title='Price (INR)',
        hovermode='x unified',
        margin=dict(t=40, b=40, l=60, r=20),
        height=500,
        template='plotly_white'
    )

    return fig

# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
