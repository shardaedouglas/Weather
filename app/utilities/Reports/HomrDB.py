import os
from flask import Flask
import oracledb
from dotenv import load_dotenv

# This file establishes a connection with the HOMR Database and acts as a store house for queries related to the CD Pubs


# Get DB Credentials from Enviornment Variables

load_dotenv()
DBusername = str(os.getenv('DB_USER'))
DBpassword = str(os.getenv('DB_PASS'))
ConnectionString = str(os.getenv('DB_CS'))


#       *************  Query list **************

# Station List for Summary of the month Query
SomQuery = "WITH main_query AS (SELECT DISTINCT A.COOP_ID, A.nws_CLIM_DIV, A.NAME_COOP_SHORT, b.TIME_OF_OBS, A.ghcnd_id, MAX(A.end_date) mshr_end_date, MAX(b.end_date) phr_end_date FROM CTXT_MSHR_GHCND_ARCHIVE A, ctxt_phr_ARCHIVE b WHERE A.nws_st_code = '04' AND b.ELEMENT IN ('TEMP', 'PRECIP') AND (A.begin_date, A.end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND (b.begin_date, b.end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND A.coop_id = b.coop_id AND b.published_flag = 'CD' AND A.report_month = (SELECT MAX(report_month) FROM ctxt_mshr_ghcnd_archive) AND b.report_month = (SELECT MAX(report_month) FROM ctxt_phr_archive) GROUP BY A.coop_id, A.nws_clim_div, A.name_coop_short, b.TIME_OF_OBS, A.ghcnd_id) SELECT coop_id, nws_clim_div, name_coop_short, time_of_obs, ghcnd_id, mshr_end_date, phr_end_date FROM (SELECT main_query.*, ROW_NUMBER() OVER (PARTITION BY main_query.coop_id ORDER BY mshr_end_date DESC, phr_end_date DESC) rn FROM main_query) WHERE rn = 1 ORDER BY nws_CLIM_DIV, coop_id"





#       ************* END Query list **************


def ConnectDB() :
    print("Initiating Connection")
    un = DBusername
    cs = ConnectionString
    pw = DBpassword
    connection = oracledb.connect(user=un, password=pw, dsn=cs)
    print("Successfully connected to Oracle Database")
    return connection

def QueryDB(query):
    getConnected = ConnectDB()
    cursor = getConnected.cursor()
    
    # Get Station list for the Summary of the Month Table
    if query == "som":
        cursor.execute(SomQuery) # Only needed 
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()

QueryDB("som")
