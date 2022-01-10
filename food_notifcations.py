from urllib.request import urlopen
from xml.etree.ElementTree import parse
import requests
from bs4 import BeautifulSoup
import pyodbc
import pandas as pd
from datetime import date, datetime, timedelta
import logging
from urllib.error import HTTPError, URLError
import socket
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import pyodbc
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import traceback
import re

logging.basicConfig(level=logging.INFO)

def db_connect():
    """Connects to DB and returns cursor of DB connection"""
    logging.info('Connecting to DB')
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
        print("Couldn't connect to DB", err)

    return cursor

def df_ref(cursor=None,override=False,min_date=date(2020, 1, 1),max_date=date(2021, 2, 1),start_page=1):

    # If cursor is not active then connect to db again
    if not cursor and not override:
        cursor = db_connect()

    # SQL to get the latest loaded date in the Db
    db_max_ref_sql = '''SELECT MAX(NotificationDate) FROM tbl_RASFF_Notifications'''
    distinct_ref = '''SELECT DISTINCT Reference FROM tbl_RASFF_Notifications'''

    old_refs_list = []
    # If just refreshing latest date then us this code, else use the provided params
    if not cursor and not override:
        try:
            cursor = db_connect()
            min_date = datetime.strptime(cursor.execute(db_max_ref_sql).fetchval(), '%Y-%m-%d').date()
            max_date = date.today()
            old_refs = cursor.execute(distinct_ref).fetchval()
            old_refs_list = old_refs['Reference'].tolist()

        except pyodbc.Error as err:
            logging.WARNING("Could not get latest date from DB")

    if cursor:
        try:
            cursor.close()
        except pyodbc.Error as err:
            logging.WARNING("Could not get latest date from DB %s", err)

    logging.info("Getting all entries from after %s", min_date)

    # Placeholder for ref and date
    df = pd.DataFrame()

    date_seek = max_date

    page = start_page
    ref = []

    # 1. Add all dates and refs from main page to DF
    # 2. Filter DF rows that are outside of given date range
    # 3. If the min date of the DF is the same as the min date provided, then the loop has gone back far enough and hence ends
    while date_seek > min_date:

        page = requests.get("https://webgate.ec.europa.eu/rasff-window/portal/?event=notificationsList&StartRow=" + str(start_page))
        start_page += 100
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all(href=True)
        re_dates = soup.find_all(text=re.compile("\d{1,2}/\d{1,2}/\d{4}"))
        dates=[]
        for item in re_dates:
            try:
                d = datetime.strptime(item.strip(), '%d/%m/%Y').date()
                dates.append(d)
            except:
                continue
        #dates = [datetime.strptime(item.strip(), '%d/%m/%Y').date() for item in re_dates]
        #print(dates)
        dates.sort()
        date_seek = dates[-3]

        logging.info('Currently back as far as: %s' % date_seek)

        if date_seek > max_date:
            continue

        for tag in links:
            url = tag.get('href')

            if 'REFERENCE' in url:
                idx = url.index('REFERENCE=')
                ref.append(url[idx+10:])


    # if old_refs_list:
    #     final_refs = [x for x in ref if x not in old_refs_list]
    # else:
    #     final_refs = []

    #print(ref)
    logging.info('Found %s entries', len(ref))

    return ref

#retrieve XML of each url. Returns list of all dictionaries of ref number and that ref number's XML
def get_xml(ref_list):
    """Given a reference number, find notification url and extract the XML from this page. Returns list of dictionaries of ref number and the XML for that ref number"""
    xmldoc_list = [] # a list of all dictionariess which contain the ref number and xml

    #ref_list = ['2014.BWM']
    i = 0
    start_time = datetime.now()

    num_ref = len(ref_list)
    logging.info('Num of refs: %s' %(num_ref))

    #cycle through list from oldest to newest
    ref_list.reverse()
    for ref in ref_list:
        ref = str(ref).upper()
        logging.info('XML for ref %s' %(ref))
        left = num_ref - i
        logging.info('%s entries remaining' %(left))

        #print(str(ref).upper())
        try:
            var_url = urlopen('https://webgate.ec.europa.eu/rasff-window/portal/?event=DetailsToXML&NOTIF_REFERENCE=' + str(ref).upper(), timeout=10)
        except HTTPError as error:
            logging.error('Data not retrieved because %s\nURL: %s', error, str(ref))
            continue
        except URLError as error:
            if isinstance(error.reason, socket.timeout):
                logging.error('Socket timed out - URL %s', str(ref))
            else:
                logging.error('Could not retrieve XML for %s', str(ref))
            continue

        # additional error handling, if no data in url then skip
        if var_url is None:
            logging.warning("Could not find XML for %s",ref)
            continue

        try:
            #xmldoc contains parsed url info
            xmldoc = parse(var_url)

            #create dictionary of ref number and the xml doc for that ref number
            xmldoc_dict = {ref:xmldoc}

            #list of these xml dictionaries
            xmldoc_list.append(xmldoc_dict)

        except:
            logging.warning("Could not create xml dictionary for ref: " + ref)
            continue

        i+=1

        #time remaining = average time * remaining entries
        current_time = datetime.now()

        tdelta = left * (current_time - start_time) / i

        logging.info('Time remaining: %s' %(tdelta))

    logging.info("# of xml docs found: %s", len(xmldoc_list))
    #list of all dictionaries of ref number and that ref number's XML
    return xmldoc_list

def xml_to_dataframe(xml_dict_doc, info):
    """Take XML and convert into dataframe. Returns a dataframe """

    # first insert data into dictionary to be later converted in dataframe
    main_dic = {}
    df_main = pd.DataFrame(columns=[
        'Subject'
        ,'Reference'
        ,'DateOfCase'
        ,'LastUpdate'
        ,'NotificationType'
        ,'NotificationCategory'
        ,'NotificationSource'
        ,'NotificationStatus'
        ,'ActionTaken'
        ,'NotificationFrom'
        ,'DistributionStatus'
        ,'Product'
        ,'ProductCategory'
        ,'RiskDecision'])
    df_hazard = pd.DataFrame(columns=[
        'ref'
        ,'subs'
        ,'cat'
        ,'anal'
        ,'unit'
        ,'sdate'])
    df_country = pd.DataFrame(columns=[
        'ref'
        ,'country'
        ,'type'])
    #key of dictionary is the reference number
    try:
        ref = str(list(xml_dict_doc.keys())[0]).upper()
        #print(ref)
    except:
        logging.WARNING("Could not retrieve reference key for dictionary" + xml_dict_doc)
        return df_main

    # return all xml from dict
    try:
        xmldoc = [xml_dict_doc[i] for i in xml_dict_doc][0]
    except:
        logging.warning("Could not retrieve xml from dictionary for ref: " + ref)
        return df_main

    # main notification data
    try:
        if info == "notifications":
            for child in xmldoc.findall('Notification/Details/'):
                main_dic[child.tag] = child.text

            # split main notification type information into separate columns. Not all notification type have the same number of information separated by "-" so this needs to be acccounted for
            notification_type = main_dic["NotificationType"].split("-")

            # first text is notification type such as "food"
            main_dic["NotificationType"] = '' if not main_dic["NotificationType"] else main_dic["NotificationType"]
            main_dic["NotificationType"] = notification_type[0].strip() if len(notification_type) > 0 else None

            # notification category such as "information for attention" or "alert"
            main_dic["NotificationCategory"] = notification_type[1].strip() if len(notification_type) > 1 else None

            # Where was the issue found? e.g. "Border Control"
            main_dic["NotificationSource"] = notification_type[2].strip() if len(notification_type) > 2 else None

            # What is the current status of the issue e.g. "Consignment released"
            main_dic["NotificationStatus"] = notification_type[3].strip() if len(notification_type) > 3 else None

            main_dic["Subject"] = '' if not main_dic["Subject"] else main_dic["Subject"]
            main_dic["Subject"] = main_dic["Subject"][500:] if len(main_dic["Subject"]) > 500 else main_dic["Subject"]

            main_dic["Reference"] = ref if ref else None

            main_dic["ActionTaken"] = '' if not main_dic["ActionTaken"] else main_dic["ActionTaken"]
            main_dic["ActionTaken"] = main_dic["ActionTaken"][255:] if len(main_dic["ActionTaken"]) > 255 else main_dic["ActionTaken"]

            main_dic["NotificationFrom"] = '' if not main_dic["NotificationFrom"] else main_dic["NotificationFrom"]
            main_dic["NotificationFrom"] = main_dic["NotificationFrom"][255:] if len(main_dic["NotificationFrom"]) > 255 else main_dic["NotificationFrom"]

            main_dic["DistributionStatus"] = '' if not main_dic["DistributionStatus"] else main_dic["DistributionStatus"]
            main_dic["DistributionStatus"] = main_dic["DistributionStatus"][1000:] if len(main_dic["DistributionStatus"]) > 1000 else main_dic["DistributionStatus"]

            main_dic["Product"] = '' if not main_dic["Product"] else main_dic["Product"]
            main_dic["Product"] = main_dic["Product"][255:] if len(main_dic["Product"]) > 255 else main_dic["Product"]

            main_dic["ProductCategory"] = '' if not main_dic["ProductCategory"] else main_dic["ProductCategory"]
            main_dic["ProductCategory"] = main_dic["ProductCategory"][255:] if len(main_dic["ProductCategory"]) > 255 else main_dic["ProductCategory"]

            main_dic["RiskDecision"] = '' if not main_dic["RiskDecision"] else main_dic["RiskDecision"]
            main_dic["RiskDecision"] = main_dic["RiskDecision"][255:] if len(main_dic["RiskDecision"]) > 255 else main_dic["RiskDecision"]

            # NA columns
            if 'PublishedInRCP' in main_dic: main_dic.pop('PublishedInRCP')

            temp_df = pd.DataFrame.from_dict([main_dic])
            df_main = pd.concat([df_main, temp_df])

            data_frame = df_main

    except Exception as e:
        logging.error("Could not retrieve DF")
        logging.error(traceback.format_exc())
        return df_main# exiting method as notification data is mandatory

    try:
        if info == "hazards":
            # hazards
            subs_list = []
            cat_list = []
            anal_list = []
            unit_list = []
            sdate_list = []

            #cycle through each item in a table and add to a list
            for child in xmldoc.findall('Notification/Hazards/row/Substance'):
                subs_list.append(child.text)
            for child in xmldoc.findall('Notification/Hazards/row/Category'):
                cat_list.append(child.text)
            for child in xmldoc.findall('Notification/Hazards/row/AnalyticalResult'):
                anal_list.append(child.text)
            for child in xmldoc.findall('Notification/Hazards/row/Units'):
                unit_list.append(child.text)
            for child in xmldoc.findall('Notification/Hazards/row/SamplingDate'):
                sdate_list.append(child.text)


            #add items to dictionary
            for t in range(len(subs_list)):

                main_dic["ref"] = ref
                main_dic["subs"] = subs_list[t]
                main_dic["cat"] = cat_list[t]
                main_dic["anal"] = anal_list[t]
                main_dic["unit"] = unit_list[t]
                main_dic["sdate"] = sdate_list[t]

                temp_df = pd.DataFrame.from_dict([main_dic])
                df_hazard = pd.concat([df_hazard, temp_df])

            data_frame = df_hazard

    except:
        # method can still continue with no hazard data
        logging.warning("Error in retrieving hazard data for: " + ref)
        return df_hazard

    try:
        if info == "countries":
            # countries
            country_list = []
            distr_list = []
            orig_list = []

            for child in xmldoc.findall('Notification/Flagged/row/Country'):
                country_list.append(child.text)
            for child in xmldoc.findall('Notification/Flagged/row/Distr'):
                distr_list.append(child.text)
            for child in xmldoc.findall('Notification/Flagged/row/Orig'):
                orig_list.append(child.text)


            for v in range(len(country_list)):

                #origin
                if orig_list[v] == "1":
                    main_dic["ref"] = ref
                    main_dic["country"] = country_list[v]
                    main_dic["type"] = 0

                #distribution
                elif distr_list[v] == "1":
                    main_dic["ref"] = ref
                    main_dic["country"] = country_list[v]
                    main_dic["type"] = 1

                #other
                else:
                    main_dic["ref"] = ref
                    main_dic["country"] = country_list[v]
                    main_dic["type"] = 2

                temp_df = pd.DataFrame.from_dict([main_dic])
                df_country = pd.concat([df_country, temp_df])

            data_frame = df_country

    except Exception as e:
        # method can still continue with no country data
        logging.warning("Error in retrieving country data for: %s", ref)
        logging.warning(e)
        return df_country

    # Create DataFrame from dictionary
    # try:
    #     data_frame = pd.DataFrame.from_dict([main_dic])
    #     #data_frame
    # except:
    #     logging.warning("Error in creating dataframe for: " + ref)

    #data_frame = data_frame[~data_frame['Reference'].isnull()]
    data_frame.fillna("",inplace=True)
    # data = data_frame.set_index("Reference")
    # data = data.drop("", axis=0)
    #logging.info("Number of DF rows for %s is %s", info, len(data_frame.index))
    return data_frame

def char_len(x, fixed_n):
    '''set string x to fixed_n character, prepend with 'xxx' if short'''
    if x:
        if len(x) > fixed_n:
            return x[:fixed_n]
        elif len(x) < fixed_n:
            return 'x' * (fixed_n - len(x)) + x
    return x

def df_dicts(xml_dict):
    """Takes in dictionary of ref num and xml for that ref number and returns dictionary of 3 key (type) value (dataframe) value pairs"""
    df_main = pd.DataFrame(columns=[
                'Subject'
                ,'Reference'
                ,'DateOfCase'
                ,'LastUpdate'
                ,'NotificationType'
                ,'NotificationCategory'
                ,'NotificationSource'
                ,'NotificationStatus'
                ,'ActionTaken'
                ,'NotificationFrom'
                ,'DistributionStatus'
                ,'Product'
                ,'ProductCategory'
                ,'RiskDecision'])
    df_hazard = pd.DataFrame(columns=[
                'ref'
                ,'subs'
                ,'cat'
                ,'anal'
                ,'unit'
                ,'sdate'])
    df_country = pd.DataFrame(columns=[
                'ref'
                ,'country'
                ,'type'])

    i = 0
    length = len(xml_dict)

    for doc in xml_dict:

        try:
            logging.info("Converting XML to DF for entry: %s. Entries left: %s", [key for key, _ in doc.items()][0], length-i)
            df_main = pd.concat([df_main, xml_to_dataframe(doc, "notifications")], axis=0)
            df_hazard = pd.concat([df_hazard, xml_to_dataframe(doc, "hazards")], axis=0)
            df_country = pd.concat([df_country, xml_to_dataframe(doc, "countries")], axis=0)

        except Exception as e:
            logging.error("Could not retrieve DF")
            logging.error(traceback.format_exc())
            continue

        i+=1

    df_main.dropna(inplace=False)
    df_hazard.dropna(inplace=True)
    df_country.dropna(inplace=True)

    # df_main['Subject'] = df_main['Subject'].apply(lambda x: char_len(x, 30))
    # df_main['Reference'] = df_main['Reference'].apply(lambda x: char_len(x, 500))
    # df_main['DateOfCase'] = df_main['DateOfCase'].apply(lambda x: char_len(x, 100))
    # df_main['LastUpdate'] = df_main['LastUpdate'].apply(lambda x: char_len(x, 100))
    # df_main['NotificationType'] = df_main['NotificationType'].apply(lambda x: char_len(x, 255))
    # df_main['NotificationCategory'] = df_main['NotificationCategory'].apply(lambda x: char_len(x, 255))
    # df_main['NotificationSource'] = df_main['NotificationSource'].apply(lambda x: char_len(x, 255))
    # df_main['NotificationStatus'] = df_main['NotificationStatus'].apply(lambda x: char_len(x, 255))
    # df_main['ActionTaken'] = df_main['ActionTaken'].apply(lambda x: char_len(x, 255))
    # df_main['NotificationFrom'] = df_main['NotificationFrom'].apply(lambda x: char_len(x, 255))
    # df_main['DistributionStatus'] = df_main['DistributionStatus'].apply(lambda x: char_len(x, 1000))
    # df_main['Product'] = df_main['Product'].apply(lambda x: char_len(x, 255))
    # df_main['ProductCategory'] = df_main['ProductCategory'].apply(lambda x: char_len(x, 255))
    # df_main['RiskDecision'] = df_main['RiskDecision'].apply(lambda x: char_len(x, 255))

    df_dict = {"notifications": df_main.dropna(inplace=False), "hazards": df_hazard.dropna(inplace=False), "countries": df_country.dropna(inplace=False)}

    logging.info("# DF rows for notifications: %s", len(df_main.index))
    logging.info("# DF rows for hazard: %s", len(df_hazard.index))
    logging.info("# DF rows for country: %s", len(df_country.index))
    return df_dict

def insert_data(df_dict, cursor=None):
    """This method takes a dictionary with 3 key (ref num) value (dataframe) pairs and inserts the dataframe data into SQL staging tables"""

    #cursor = db_connect()

    notifications_sql = '''
        INSERT INTO [stg_RASFF_Notifications] (
                [Subject]
                , [Reference]
                , [NotificationDate]
                , [LastUpdate]
                , [NotificationType]
                , [NotificationCategory]
                , [NotificationSource]
                , [NotificationStatus]
                , [ActionTaken]
                , [NotificationFrom]
                , [DistributionStatus]
                , [Product]
                , [ProductCategory]
                , [RiskDecision]) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''

    hazards_sql = '''
        INSERT INTO [stg_RASFF_Notifications_SubstanceHazard] (
                [Reference]
                , [SubstanceHazard]
                , [Category]
                , [AnalyticalResult]
                , [Units]
                , [SamplingDate]) 
                VALUES (?, ?, ?, ?, ?, ?)
            '''

    countries_sql = '''INSERT INTO [stg_RASFF_Notifications_Country] (
            [Reference]
            , [Country]
            , [Type]) 
            VALUES (?, ?, ?)'''

    df_list = ["notifications","hazards","countries"]
    sql_list = [notifications_sql, hazards_sql, countries_sql]

    cursor = db_connect()
    sql_not_count = '''SELECT COUNT(*) FROM stg_RASFF_Notifications'''
    pre_not_count = cursor.execute(sql_not_count).fetchval()

    try:
        for df,sql in zip(df_list,sql_list):
            dataframe = df_dict[df]
            # print(dataframe)
            cursor = db_connect()
            try:
                for _,row in dataframe.iterrows():
                    # print(row)
                    # tp_args = tuple(row)
                    # ag = [str(item).capitalize() for item in tp_args]
                    args = tuple(row)
                    # print(args)
                    # print(sql)
                    # print(args)
                    try:
                        #logging.info("Loading %s rows into staging for %s", len(params),df)
                        cursor.execute(sql, args)


                    except:
                        logging.warning("Could not execute SQL %s", args)
                        continue

                    try:
                        cursor.commit()
                    except:
                        logging.warning("Could not commit SQL", args)
                        continue


            except pyodbc.DatabaseError as err:
                logging.warning("Could not execute SQL %s", err)
                cursor.rollback()
            # else:
            #     cursor.commit()
            #     logging.info("SQL Committed")
            finally:
                cursor.close()

    except:
        logging.error("Unable to populate staging")

    cursor = db_connect()
    sql_not_count = '''SELECT COUNT(*) FROM stg_RASFF_Notifications'''
    post_not_count = cursor.execute(sql_not_count).fetchval()
    cursor.close()
    logging.info("Number of entries added: %s", (post_not_count-pre_not_count))

def table_update(cursor=None):
    """This method takes all data in the staging tables and merges it into the interface tables"""

    logging.info("Merging staging with production")
    try:
        cursor.execute('''SELECT MAX(Reference) FROM tbl_RASFF_Notifications''')
    except:
        cursor = db_connect()

    sql_not_count = '''SELECT COUNT(*) FROM tbl_RASFF_Notifications'''
    pre_not_count = cursor.execute(sql_not_count).fetchval()

    notification_merge_sql = '''
        MERGE
        tbl_RASFF_Notifications AS T
        USING (
            SELECT DISTINCT
                 N.Reference
                ,N.Subject
                ,CONVERT(DATE, N.NotificationDate, 103) AS NotificationDate
                ,CONVERT(DATE, N.LastUpdate, 103) AS LastUpdate
                ,N.NotificationType
                ,N.NotificationCategory
                ,N.NotificationSource
                ,N.NotificationStatus
                ,N.ActionTaken
                ,N.NotificationFrom
                ,N.DistributionStatus
                ,N.Product
                ,N.ProductCategory
                ,N.RiskDecision
            FROM stg_RASFF_Notifications N
            WHERE len(N.Reference) > 1) AS S
        ON T.Reference = S.Reference
        WHEN MATCHED THEN
            UPDATE SET
                T.Reference = S.Reference
                ,T.Subject = S.Subject
                ,T.NotificationDate = S.NotificationDate
                ,T.LastUpdate = S.LastUpdate
                ,T.NotificationType = S.NotificationType
                ,T.NotificationCategory = S.NotificationCategory
                ,T.NotificationSource = S.NotificationSource
                ,T.NotificationStatus = S.NotificationStatus
                ,T.ActionTaken = S.ActionTaken
                ,T.NotificationFrom = S.NotificationFrom
                ,T.DistributionStatus = S.DistributionStatus
                ,T.Product = S.Product
                ,T.ProductCategory = S.ProductCategory
                ,T.RiskDecision = S.RiskDecision
        WHEN NOT MATCHED THEN
            INSERT (
                 Reference 
                ,Subject 
                ,NotificationDate 
                ,LastUpdate
                ,NotificationType 
                ,NotificationCategory
                ,NotificationSource 
                ,NotificationStatus 
                ,ActionTaken 
                ,NotificationFrom 
                ,DistributionStatus 
                ,Product
                ,ProductCategory 
                ,RiskDecision)
            VALUES (
                 S.Reference
                ,S.Subject
                ,S.NotificationDate
                ,S.LastUpdate
                ,S.NotificationType
                ,S.NotificationCategory
                ,S.NotificationSource
                ,S.NotificationStatus
                ,S.ActionTaken
                ,S.NotificationFrom
                ,S.DistributionStatus
                ,S.Product
                ,S.ProductCategory
                ,S.RiskDecision);'''

    country_merge_sql = '''
        DELETE B
        FROM stg_RASFF_Notifications_Country A
            LEFT JOIN tbl_RASFF_Notifications_Country B ON A.Reference = B.Reference
        WHERE B.Reference IS NOT NULL
        

        INSERT INTO tbl_RASFF_Notifications_Country (Id
            ,Reference
            ,Country
            ,Type)
        SELECT CHECKSUM(NEWID())
            ,SUB.Reference
            ,SUB.Country
            ,SUB.Type
        FROM (
            SELECT DISTINCT
            C.Reference
            ,C.Country
            ,C.Type
        FROM stg_RASFF_Notifications_Country C
            INNER JOIN tbl_RASFF_Notifications N ON N.Reference = C.Reference) SUB
        '''

    hazard_merge_sql = '''
        DELETE B
        FROM stg_RASFF_Notifications_SubstanceHazard A
            LEFT JOIN tbl_RASFF_Notifications_SubstanceHazard B ON A.Reference = B.Reference
        WHERE B.Reference IS NOT NULL
        
        INSERT INTO tbl_RASFF_Notifications_SubstanceHazard (
            Id
            ,Reference
            ,SubstanceHazard
            ,Category
            ,AnalyticalResult
            ,Units
            ,SamplingDate)
        SELECT CHECKSUM(NEWID())
            ,H.Reference
            ,H.SubstanceHazard
            ,H.Category
            ,H.AnalyticalResult
            ,H.Units
            ,H.SamplingDate
        FROM (
        SELECT DISTINCT
            H.Reference
            ,H.SubstanceHazard
            ,H.Category
            ,H.AnalyticalResult
            ,H.Units
            ,CONVERT(DATE, H.SamplingDate, 105) AS SamplingDate
        FROM stg_RASFF_Notifications_SubstanceHazard H
            INNER JOIN tbl_RASFF_Notifications N ON N.Reference = H.Reference) H
        '''

    try:
        cursor = db_connect()
    except pyodbc.DatabaseError as err:
        logging.error("Could not connect to the DB", err)
        return

    sql_list = ["Notification", "Country","Hazard"]
    for item, sql in zip(sql_list,[notification_merge_sql,country_merge_sql,hazard_merge_sql]):

        logging.info("Merging %s",item)
        try:
            cursor.execute(sql)
        except pyodbc.DatabaseError as err:
            logging.warning("Could not execute SQL %s", err)
            cursor.rollback()

    try:
        cursor.commit()
        post_not_count = cursor.execute(sql_not_count).fetchval()
    except pyodbc.DatabaseError as err:
        cursor.rollback()
    finally:
        cursor.close()

    logging.info('%s notifications added', (post_not_count-pre_not_count))

def db_initialization(cursor=None):
    """This method sets up the DB tables for later use"""

    # SQL for creating staging and interface tables
    # notification staging table
    stg_noti_tbl = '''
        BEGIN TRY
        
            BEGIN TRANSACTION
                --If this table does not exist then create it
                IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NULL
                    BEGIN
                        CREATE TABLE [stg_RASFF_Notifications](
                        [Reference] VARCHAR(100) NOT NULL,
                        [Subject] NVARCHAR(500) NULL,
                        [NotificationDate] NVARCHAR(100) NULL,
                        [LastUpdate] NVARCHAR(100) NULL,
                        [NotificationType] NVARCHAR(255) NULL,
                        [NotificationCategory] NVARCHAR(255) NULL,
                        [NotificationSource] NVARCHAR(255) NULL,
                        [NotificationStatus] NVARCHAR(255) NULL,
                        [ActionTaken] NVARCHAR(255) NULL,
                        [NotificationFrom] NVARCHAR(255) NULL,
                        [DistributionStatus] NVARCHAR(1000) NULL,
                        [Product] NVARCHAR(255) NULL,
                        [ProductCategory] NVARCHAR(255) NULL,
                        [RiskDecision] NVARCHAR(255) NULL)
                    END
        
            COMMIT TRAN
        
        END TRY
        
        BEGIN CATCH
        
            IF @@TRANCOUNT > 0
                ROLLBACK TRAN
        
        END CATCH
    '''

    # hazard staging table
    stg_haz_tbl = '''
    BEGIN TRY

        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
                BEGIN
                    CREATE TABLE [stg_RASFF_Notifications_SubstanceHazard](
                        [Reference] VARCHAR(100) NOT NULL,
                        [SubstanceHazard] NVARCHAR(255) NULL,
                        [Category] NVARCHAR(255) NULL,
                        [AnalyticalResult] NVARCHAR(255) NULL,
                        [Units] NVARCHAR(255) NULL,
                        [SamplingDate] NVARCHAR(10) NULL) 
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # country staging table
    stg_cntry_tbl = '''
    BEGIN TRY

        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [stg_RASFF_Notifications_Country](
                        [Reference] VARCHAR(255) NOT NULL,
                        [Country] NVARCHAR(255) NULL,
                        [Type] BIT NOT NULL
                    )
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # notification interface table
    int_noti_tbl = '''
    --Notifications
    BEGIN TRY
    
        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [tbl_RASFF_Notifications](
                        [Reference] VARCHAR(30) NOT NULL,
                        [Subject] NVARCHAR(500) NULL,
                        [NotificationDate] DATE NULL,
                        [LastUpdate] DATE NULL,
                        [NotificationType] NVARCHAR(255) NULL,
                        [NotificationCategory] NVARCHAR(255) NULL,
                        [NotificationSource] NVARCHAR(255) NULL,
                        [NotificationStatus] NVARCHAR(255) NULL,
                        [ActionTaken] NVARCHAR(255) NULL,
                        [NotificationFrom] NVARCHAR(255) NULL,
                        [DistributionStatus] NVARCHAR(1000) NULL,
                        [Product] NVARCHAR(255) NULL,
                        [ProductCategory] NVARCHAR(255) NULL,
                        [RiskDecision] NVARCHAR(255) NULL
    
                    PRIMARY KEY CLUSTERED 
                    (
                        [Reference] ASC
                    ))
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # Hazard interface table
    int_haz_tbl = '''
    --Hazards
    BEGIN TRY
    
        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
                BEGIN
                    
                    
                    CREATE TABLE [tbl_RASFF_Notifications_SubstanceHazard](
                        [Id] BIGINT NOT NULL,
                        [Reference] VARCHAR(30) NOT NULL,
                        [SubstanceHazard] NVARCHAR(255) NULL,
                        [Category] NVARCHAR(255) NULL,
                        [AnalyticalResult] NVARCHAR(255) NULL,
                        [Units] NVARCHAR(255) NULL,
                        [SamplingDate] DATE NULL,
                        PRIMARY KEY([Id]),
                        FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                    ) 
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH 
    '''

    # Country interface table
    int_cntry_tbl = '''   
    BEGIN TRY

	BEGIN TRANSACTION

            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [tbl_RASFF_Notifications_Country](
                        [Id] BIGINT NOT NULL,
                        [Reference] VARCHAR(30) NOT NULL,
                        [Country] NVARCHAR(255) NULL,
                        [Type] BIT NOT NULL
                        PRIMARY KEY([Id]),
                        FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                    ) 
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    tbl_ar = [stg_noti_tbl
              ,stg_haz_tbl
              ,stg_cntry_tbl
              ,int_noti_tbl
              ,int_haz_tbl
              ,int_cntry_tbl]

    for tbl in tbl_ar:
        try:
            cursor = db_connect()
            cursor.execute(tbl)

        except pyodbc.DatabaseError as err:
            cursor.rollback()
        else:
            cursor.commit()
            logging.info("Tables Created")
        finally:
            cursor.close()

def clear_db(cursor=None,action="all"):
    """Clears data in the DB either by truncating tables or deleting them or both"""

    if not cursor:
        cursor = db_connect()

    # SQL to truncate tables (tbl_RASFF_Notifications  cannot be truncate and must be deleted due to key references)
    truncate_tbls = '''
        --Truncate all tables
        BEGIN TRY
        
            BEGIN TRANSACTION
        
                --Truncate main notification table is exists. Cannot truncate as it has a key reference
                IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NOT NULL
                    BEGIN
                        
                        DELETE FROM [tbl_RASFF_Notifications]
                        
                    END
                    
                --Truncate main country table is exists
                IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NOT NULL
                    BEGIN
                        
                        TRUNCATE TABLE [tbl_RASFF_Notifications_Country]
                        
                    END
                    
                --Truncate main hazard table is exists
                IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
                    BEGIN
                            
                        TRUNCATE TABLE [tbl_RASFF_Notifications_SubstanceHazard]
                        
                    END
        
        
                --Truncate staging notification table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NOT NULL
                    BEGIN
                        
                        TRUNCATE TABLE [stg_RASFF_Notifications]
                        
                    END
                    
                --Truncate staging country table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NOT NULL
                    BEGIN
                        
                        TRUNCATE TABLE [stg_RASFF_Notifications_Country]
                        
                    END
                    
                --Truncate staging hazard table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
                    BEGIN
                            
                        TRUNCATE TABLE [stg_RASFF_Notifications_SubstanceHazard]
                        
                    END
        
            COMMIT TRAN
        
        END TRY
        
        BEGIN CATCH
        
            IF @@TRANCOUNT > 0
                ROLLBACK TRAN
        
        END CATCH
    
    '''

    # SQL to delete tables
    delete_tbls = '''--Delete all tables
        BEGIN TRY
        
            BEGIN TRANSACTION
        
                --Delete interface country table is exists
                IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NOT NULL
                    BEGIN
                        
                        DROP TABLE [tbl_RASFF_Notifications_Country]
                        
                    END
                    
                --Delete interface hazard table is exists
                IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
                    BEGIN
                            
                        DROP TABLE [tbl_RASFF_Notifications_SubstanceHazard]
                        
                    END
        
                --Delete interface notification table is exists
                IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NOT NULL
                    BEGIN
                        
                        DROP TABLE [tbl_RASFF_Notifications]
                        
                    END
                    
        
                --Delete staging notification table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NOT NULL
                    BEGIN
                        
                        DROP TABLE [stg_RASFF_Notifications]
                        
                    END
                    
                --Delete staging country table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NOT NULL
                    BEGIN
                        
                        DROP TABLE [stg_RASFF_Notifications_Country]
                        
                    END
                    
                --Delete staging hazard table is exists
                IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
                    BEGIN
                            
                        DROP TABLE [stg_RASFF_Notifications_SubstanceHazard]
                        
                    END
        
            COMMIT TRAN
        
        END TRY
        
        BEGIN CATCH
        
            IF @@TRANCOUNT > 0
                ROLLBACK TRAN
        
        END CATCH
                '''

    if action == "all":
        sql = truncate_tbls + delete_tbls

    if action == "truncate":
        sql = truncate_tbls

    if action == "delete":
        sql = delete_tbls

    try:
        cursor.execute(sql)
    except pyodbc.Error as err:
        print("Error on execution of SQL \n" + sql)

    try:
        cursor.commit()

    except pyodbc.Error as err:
        print("Error committing SQL transaction", err)

def db_initialization(cursor=None):
    """This method sets up the DB tables for later use"""

    if not cursor:
        cursor = db_connect()

    # SQL for creating staging and interface tables
    # notification staging table
    stg_noti_tbl = '''
        BEGIN TRY
        
            BEGIN TRANSACTION
                --If this table does not exist then create it
                IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NULL
                    BEGIN
                        CREATE TABLE [stg_RASFF_Notifications](
                        [Reference] VARCHAR(100) NOT NULL,
                        [Subject] NVARCHAR(500) NULL,
                        [NotificationDate] NVARCHAR(100) NULL,
                        [LastUpdate] NVARCHAR(100) NULL,
                        [NotificationType] NVARCHAR(255) NULL,
                        [NotificationCategory] NVARCHAR(255) NULL,
                        [NotificationSource] NVARCHAR(255) NULL,
                        [NotificationStatus] NVARCHAR(255) NULL,
                        [ActionTaken] NVARCHAR(255) NULL,
                        [NotificationFrom] NVARCHAR(255) NULL,
                        [DistributionStatus] NVARCHAR(1000) NULL,
                        [Product] NVARCHAR(255) NULL,
                        [ProductCategory] NVARCHAR(255) NULL,
                        [RiskDecision] NVARCHAR(255) NULL)
                    END
        
            COMMIT TRAN
        
        END TRY
        
        BEGIN CATCH
        
            IF @@TRANCOUNT > 0
                ROLLBACK TRAN
        
        END CATCH
    '''

    # hazard staging table
    stg_haz_tbl = '''
    BEGIN TRY

        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
                BEGIN
                    CREATE TABLE [stg_RASFF_Notifications_SubstanceHazard](
                        [Reference] VARCHAR(100) NOT NULL,
                        [SubstanceHazard] NVARCHAR(255) NULL,
                        [Category] NVARCHAR(255) NULL,
                        [AnalyticalResult] NVARCHAR(255) NULL,
                        [Units] NVARCHAR(255) NULL,
                        [SamplingDate] NVARCHAR(10) NULL) 
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # country staging table
    stg_cntry_tbl = '''
    BEGIN TRY

        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [stg_RASFF_Notifications_Country](
                        [Reference] VARCHAR(255) NOT NULL,
                        [Country] NVARCHAR(255) NULL,
                        [Type] BIT NOT NULL
                    )
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # notification interface table
    int_noti_tbl = '''
    --Notifications
    BEGIN TRY
    
        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [tbl_RASFF_Notifications](
                        [Reference] VARCHAR(100) NOT NULL,
                        [Subject] NVARCHAR(500) NULL,
                        [NotificationDate] DATE NULL,
                        [LastUpdate] DATE NULL,
                        [NotificationType] NVARCHAR(255) NULL,
                        [NotificationCategory] NVARCHAR(255) NULL,
                        [NotificationSource] NVARCHAR(255) NULL,
                        [NotificationStatus] NVARCHAR(255) NULL,
                        [ActionTaken] NVARCHAR(255) NULL,
                        [NotificationFrom] NVARCHAR(255) NULL,
                        [DistributionStatus] NVARCHAR(1000) NULL,
                        [Product] NVARCHAR(255) NULL,
                        [ProductCategory] NVARCHAR(255) NULL,
                        [RiskDecision] NVARCHAR(255) NULL
    
                    PRIMARY KEY CLUSTERED 
                    (
                        [Reference] ASC
                    ))
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    # Hazard interface table
    int_haz_tbl = '''
    --Hazards
    BEGIN TRY
    
        BEGIN TRANSACTION
    
            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
                BEGIN
                    
                    
                    CREATE TABLE [tbl_RASFF_Notifications_SubstanceHazard](
                        [Id] BIGINT NOT NULL,
                        [Reference] VARCHAR(100) NOT NULL,
                        [SubstanceHazard] NVARCHAR(255) NULL,
                        [Category] NVARCHAR(255) NULL,
                        [AnalyticalResult] NVARCHAR(255) NULL,
                        [Units] NVARCHAR(255) NULL,
                        [SamplingDate] DATE NULL,
                        PRIMARY KEY([Id]),
                        FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                    ) 
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH 
    '''

    # Country interface table
    int_cntry_tbl = '''   
    BEGIN TRY

	BEGIN TRANSACTION

            --If this table does not exist then create it
            IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NULL
                BEGIN
                    
                    CREATE TABLE [tbl_RASFF_Notifications_Country](
                        [Id] BIGINT NOT NULL,
                        [Reference] VARCHAR(100) NOT NULL,
                        [Country] NVARCHAR(255) NULL,
                        [Type] BIT NOT NULL
                        PRIMARY KEY([Id]),
                        FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                    ) 
                    
                END
    
        COMMIT TRAN
    
    END TRY
    
    BEGIN CATCH
    
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN
    
    END CATCH
    '''

    tbl_ar = [stg_noti_tbl
              ,stg_haz_tbl
              ,stg_cntry_tbl
              ,int_noti_tbl
              ,int_haz_tbl
              ,int_cntry_tbl]

    for tbl in tbl_ar:
        try:
            cursor.execute(tbl)
        except pyodbc.Error as err:
            print("Error on execution of SQL \n" + tbl)

    try:
        cursor.commit()

    except pyodbc.Error as err:
        print("Error commiting SQL transaction")

    # return cursor for use by other methods
    return cursor

if __name__ == "__main__":

    #load by month
    page = 49000
    for i in range(10):
        # for j in [10,8,6,4,2,1]:
        for j in [11,10,9,8,7,6,5,4,3,2,1]:
            s_date = date(2006-i, 0+j, 1)
            e_date = date(2006-i, 0+j, 28 if j == 2 else 30)
            # s_date = date(2008, 1, 1)
            # e_date = date(2008, 2, 1)
            print(s_date)
            print(e_date)
            ref_list = df_ref(override=True, min_date=s_date, max_date=e_date, start_page=page)
            #ref_list = ['2020.5185']
            page = page + len(ref_list)
            xml = get_xml(ref_list)
            df_dicts1 = df_dicts(xml)
            insert_data(df_dicts1)

    # table_update()
    # ref_list = ['1998.EM']
    # xml = get_xml(ref_list)
    # df_dicts1 = df_dicts(xml)
    # insert_data(df_dicts1)
    # # table_update()
    # clear_db()
    # db_initialization()
