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
SomQuery = "WITH main_query AS (SELECT DISTINCT A.COOP_ID, A.nws_CLIM_DIV, A.NAME_COOP_SHORT, b.TIME_OF_OBS, A.ghcnd_id, MAX(A.end_date) mshr_end_date, MAX(b.end_date) phr_end_date FROM CTXT_MSHR_GHCND_ARCHIVE A, ctxt_phr_ARCHIVE b WHERE A.nws_st_code = '04' AND b.ELEMENT IN ('TEMP', 'PRECIP', 'MAX/MINTEM', 'EVAP', 'WIND', 'TEMPATOBS') AND (A.begin_date, A.end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND (b.begin_date, b.end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND A.coop_id = b.coop_id AND b.published_flag = 'CD' AND A.report_month = (SELECT MAX(report_month) FROM ctxt_mshr_ghcnd_archive) AND b.report_month = (SELECT MAX(report_month) FROM ctxt_phr_archive) GROUP BY A.coop_id, A.nws_clim_div, A.name_coop_short, b.TIME_OF_OBS, A.ghcnd_id) SELECT coop_id, nws_clim_div, name_coop_short, time_of_obs, ghcnd_id, mshr_end_date, phr_end_date FROM (SELECT main_query.*, ROW_NUMBER() OVER (PARTITION BY main_query.coop_id ORDER BY mshr_end_date DESC, phr_end_date DESC) rn FROM main_query) WHERE rn = 1 ORDER BY nws_CLIM_DIV, coop_id"
TobsQuery = "WITH main_query AS (SELECT coop_id, data_program, ELEMENT, TIME_OF_OBS, EQUIPMENT, equipment_mods, equipment_exposure, DECODE(REGEXP_INSTR(phr.equipment_mods,'*RCRD*'),0,NULL,NULL,NULL,'R') recording, DECODE(REGEXP_INSTR(phr.equipment_mods,'*SHLD*'),0,NULL,NULL,NULL,'//') shield_flag, DECODE(REGEXP_INSTR(phr.equipment_exposure,'*ROOF*'),0,NULL,NULL,NULL,'#') roof_flag, end_date, NVL2(lcd.wban_id, 'J', NULL) j_flag FROM ctxt_phr_archive phr LEFT OUTER JOIN mmsref.nws_first_order_lcd lcd ON phr.wban_id = lcd.wban_id WHERE report_month = (SELECT MAX(report_month) FROM ctxt_phr_archive) AND (begin_date, end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND published_flag = 'CD' AND data_program = 'COOP SOD' AND SUBSTR(coop_id,1,2) = '04'), hpd_collocated AS (SELECT coop_id, published_flag FROM ctxt_phr_archive WHERE (begin_date, end_date) OVERLAPS (DATE '2023-02-01', DATE '2023-02-28') AND report_month = (SELECT MAX(report_month) FROM ctxt_phr_archive) AND published_flag = 'HPD' GROUP BY coop_id, published_flag) SELECT coop_id, data_program, ELEMENT, TIME_OF_OBS, EQUIPMENT, shield_flag, roof_flag, j_flag, CASE WHEN equipment IN ('PCPNX','PCPN1') AND hpd_collocated IS NOT NULL THEN 'R' ELSE recording END r_flag, CASE WHEN ELEMENT = 'PRECIP' AND recording IS NULL AND equipment NOT IN ('PCPNX','PCPN1') AND hpd_collocated IS NOT NULL THEN 'C' ELSE NULL END c_flag, CASE WHEN ELEMENT IN ('MAX/MINTEM','TEMPATOBS') THEN 'G' ELSE NULL END g_flag FROM (SELECT main_query.*, hpd_collocated.published_flag hpd_collocated, ROW_NUMBER() OVER (PARTITION BY main_query.coop_id, data_program, ELEMENT ORDER BY end_date DESC, equipment_mods, equipment_exposure) rn FROM main_query LEFT OUTER JOIN hpd_collocated ON main_query.coop_id = hpd_collocated.coop_id) WHERE rn = 1 ORDER BY coop_id"

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
        
def QuerySoM(query):
    connection = ConnectDB()
    cursor = connection.cursor()
    
    if query == "som":
        cursor.execute(SomQuery)
        rows = cursor.fetchall()
    elif query == "tobs":
        cursor.execute(TobsQuery)
        rows = cursor.fetchall()
    else:
        rows = []

    cursor.close()
    connection.close()
    return rows


# QueryDB("som")
