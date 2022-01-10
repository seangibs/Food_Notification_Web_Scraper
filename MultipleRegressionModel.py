from sklearn.linear_model import LinearRegression
import logging
import statsmodels.api as sm
import pyodbc
import pandas as pd

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

def regression_model():
    """Create Multiple Regression Model"""
    conn = db_connect(conn_connect=True)

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

    noti_smoke_GDP_df = pd.read_sql(noti_smoke_GDP_sql, conn)

    noti_smoke_GDP_df["Value"] = pd.to_numeric(noti_smoke_GDP_df["Value"])


    X = noti_smoke_GDP_df[["NumInstances","Perc_Smoke"]]
    y = noti_smoke_GDP_df[["Value"]]

    regressor = LinearRegression()
    regressor.fit(X, y)

    X = noti_smoke_GDP_df[["NumInstances","Perc_Smoke"]]
    y = noti_smoke_GDP_df[["Value"]]

    model = sm.OLS(y, X).fit()

    print(model.summary())

if __name__ == '__main__':

    regression_model()
