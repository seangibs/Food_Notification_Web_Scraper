import logging
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import pyodbc
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc

logging.basicConfig(level=logging.INFO)


def db_connect(conn_connect=False):
    """Connects to DB and returns cursor of DB connection"""
    # logging.info('Connecting to DB')
    # establish connection to db
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=dbanalyticspg12.database.windows.net;'
                              'Database=Hazards;'
                              'UID=SeanGibbons;'
                              'PWD=e0xcjr589GY23')
        cursor = conn.cursor()
        logging.info('Successfully connected to DB')
    except pyodbc.Error as err:
        logging.warning("Couldn't connect to DB", err)

    if conn_connect:
        return conn
    else:
        return cursor
#
def display_df():
    """Used to retrieve data from the DB to be shown in the dashboard"""
    conn = db_connect(conn_connect=True)

    notify_GDP_sql = '''
    SELECT G.Country_Name AS Country
        ,G.Value 
        ,CAST(G.[Year] AS INT) AS [Year]
        ,COUNT(DISTINCT N.Reference) AS NumInstances
    FROM tbl_RASFF_Notifications N
        INNER JOIN GDP G ON G.Country_Name = LEFT(N.NotificationFrom,CHARINDEX('(', N.NotificationFrom)-2)
            AND YEAR(N.NotificationDate) = G.[Year]
    GROUP BY G.Country_Name
        ,G.Value 
        ,G.[Year]
        '''

    smoke_GDP_sql = '''
    SELECT T.[Year] 
        ,G.Country_Name AS Country
        ,G.Value 
        ,AVG(T.Data_Value) AS Perc_Smoke
    FROM Cleansed_Global_Tobacco_Data T
        INNER JOIN GDP G ON G.Country_Name = T.Country
            AND G.[Year] = T.[Year]
    WHERE T.Indicator = 'Percentage of youth who currently smoke cigarettes'
        GROUP BY T.[Year] 
        ,G.Country_Name
        ,G.Value 
            ORDER BY 1,2
        '''

    noti_smoke_GDP_sql = '''
    SELECT G.Country_Name AS Country
        ,G.Value 
        ,COUNT(DISTINCT N.Reference) AS NumInstances
        ,AVG(T.Data_Value) AS Perc_Smoke
    FROM tbl_RASFF_Notifications N
        INNER JOIN GDP G ON G.Country_Name = LEFT(N.NotificationFrom,CHARINDEX('(', N.NotificationFrom)-2)
            AND YEAR(N.NotificationDate) = G.[Year]
        INNER JOIN Cleansed_Global_Tobacco_Data T ON G.Country_Name = T.Country
            AND G.[Year] = T.[Year]
    GROUP BY G.Country_Name
        ,G.Value 
        '''

    notify_GDP_df = pd.read_sql(notify_GDP_sql, conn)
    smoke_GDP_df = pd.read_sql(smoke_GDP_sql, conn)
    noti_smoke_GDP_df = pd.read_sql(noti_smoke_GDP_sql, conn)

    notify_GDP_df["Value"] = pd.to_numeric(notify_GDP_df["Value"])
    smoke_GDP_df["Value"] = pd.to_numeric(smoke_GDP_df["Value"])
    noti_smoke_GDP_df["Value"] = pd.to_numeric(noti_smoke_GDP_df["Value"])

    nsg = noti_smoke_GDP_df.groupby("Country",as_index=False).mean()


    return [notify_GDP_df,smoke_GDP_df,nsg]

df_ar = display_df()
df = df_ar[0]
smoke_df = df_ar[1]
noti_smoke_GDP_df = df_ar[2]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}]
                )

app.layout = dbc.Container([

    dbc.Row(
        dbc.Col(html.H1("RASFF Notification - Youth Smoking - GDP",
                        className='text-center mb-4'),
                width=12)
    ),

    dbc.Row([
        dbc.Col([
            html.P(" "),
            dcc.RangeSlider(
                id='year_slider',
                min=min(df["Year"]),
                max=max(df["Year"]),
                value=[i for i in df["Year"].unique()],
                marks={int(i): str(i) for i in df["Year"].unique()},
                step=None)]
            , width=10)], no_gutters=False, justify='center'
    ),

    dbc.Row([html.P(" ")]),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='my_map', figure={})]
            , width=10)], no_gutters=False, justify='center'
    ),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='boxplot', figure={})]
            , width=10)
        ], no_gutters=False
    ),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='all_plot', figure={})]
            , width=10)
        ], no_gutters=False
    ),


], fluid=False)

@app.callback(

    [Output(component_id='my_map', component_property='figure'),
     Output(component_id='boxplot', component_property='figure'),
     Output(component_id='all_plot', component_property='figure')
     ],
    [
     Input(component_id='year_slider', component_property='value')
     ]
)

def update_graph(year_slider):

    dff = df.copy()
    dff = dff[(dff['Year'] > min(year_slider)) & (dff['Year'] <= max(year_slider))]


    smoke_GDP = px.histogram(smoke_df
                        , nbins=30
                        , y = smoke_df['Perc_Smoke']
                        , x = smoke_df['Value']
                       , histnorm='probability density')

    all_plot = px.scatter(noti_smoke_GDP_df
                            , x="NumInstances"
                            , y="Perc_Smoke"
                            , size="Value"
                            , color="Country"
                            , log_x=True
                            , size_max=60)


    rasff_GDP = px.histogram(dff
                    , nbins=30
                    , y = dff['NumInstances']
                    , x = dff['Value']
                   , histnorm='density'
                )

    return rasff_GDP, smoke_GDP, all_plot

if __name__ == '__main__':
    app.run_server(debug=True, port=8000)
