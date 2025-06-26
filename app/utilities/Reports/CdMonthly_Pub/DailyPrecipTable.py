
import sys
import os

# Get the absolute path to the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..','..'))
print(parent_dir)
sys.path.insert(0, parent_dir)

# Now you can import from the parent directory
import polars as pl
import json, os
from collections import defaultdict
import calendar
from calendar import monthrange
from datetime import datetime
from app.utilities.Reports.HomrDB import ConnectDB, QueryDB, QuerySoM, DailyPrecipQuery
from app.dataingest.readandfilterGHCN import parse_and_filter
import traceback

#Testing Daily Precip Query

def get_mm_to_in(mm: float) -> float:
    """Convert millimeters to inches."""
    return mm * 0.03937 # 1 inch = 25.4 mm


# def generateDailyPrecip():
def generateDailyPrecip(month:int = 9, year:int = 2020) -> dict:
    """ Generates Daily Precipitation and Total Precipitation for a state.

    Parameters
    ----------
    month : int, optional
        by default 9
    year : int, optional
        by default 2020

    Returns
    ---------
    daily_precip_table_rec : dict[dict]
        Records with their GHCN-ID as the key. Contains total_pcn, daily_pcn

    Notes
    ---------

    This should eventually be updated to recieve the month year (and possibly state) codes so they aren't hardcoded. 

    """

    
    # Get Station List
    db_stations = QueryDB(DailyPrecipQuery)
    print(bool(db_stations))
    for station in db_stations:
        print(station)

    all_filtered_dfs = []
    noDataCount = 0
    full_station_id_list = []

    for row in db_stations[:10]:
    # for row in db_stations:
        ghcn_id = row[4]
        file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"
        full_station_id_list.append(ghcn_id)

        if not os.path.exists(file_path):
            print(f"Missing file: {file_path}")
            continue

        try:
            filtered_data = parse_and_filter(
                station_code=ghcn_id,
                file_path=file_path,
                correction_type="table",
                month=month,
                year=year
            )

            filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

            if filtered_df.is_empty():
                print(f"Skipping station {ghcn_id} due to no data.")
                noDataCount += 1
                continue

            if all_filtered_dfs:
                existing_columns = all_filtered_dfs[0].columns
                current_columns = filtered_df.columns

                missing_columns = set(existing_columns) - set(current_columns)
                for col in missing_columns:
                    filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

                filtered_df = filtered_df.select(existing_columns)

            all_filtered_dfs.append(filtered_df)
            print(f"Parsed {len(filtered_df)} records from {ghcn_id}")

        except Exception as e:
            print(f"Error parsing {ghcn_id}: {e}")
            continue

    if not all_filtered_dfs:
        print("No valid station files found.")
        # return?
    
        

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)

    # Optional: write JSON string to file
    output_file = f"combined_data_{month}_{year}.json"
    with open(output_file, "w") as f:
        f.write(json_data)
    
    
    
    print(f"Data saved to {output_file}")
    # print(json_data)


    ########################################
    #  Read the JSON file for Testing

    # json_data = None

    # with open(output_file) as f:
    #     json_data = json.load(f)
    #     # print(d)

    #########################################

    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"]
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
            

    for key, data in prcp_data.items():
        # print(f"{key}:\t{data}")
        write_to_file(f"{key}:\t{data}")
    write_to_file(f"-"*30)
    for key, data in mdpr_data.items():
        # print(f"{key}:\t{data}")
        
        write_to_file(f"{key}:\t{data}")
        write_to_file(len(data))
    write_to_file(f"-"*30)
    for key, data in dapr_data.items():
        # print(f"{key}:\t{data}")
        
        write_to_file(f"{key}:\t{data}")
        write_to_file(len(data))

    # print(prcp_data)
    ############################################


    # prcp_data = {
    #     'test_station_01': [(0, '  7'), (0, '  7'), (297, '  7'), (157, '  7'), (-9999, '   ') , (-9999, '   ') , (0, '  7'), (0, '  7'), (137, '  7'), (0, '  7'), (51, '  7'), (0, '  7'), (0, 'T 7') , (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (8, '  7'), (424, '  7'), (0, 'T 7'), (272, '  7'), (244, '  7'), (-9999, '   ')] 
    #     }
    # mdpr_data = {
    #     'test_station_01': [(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (46, '  7'), (46, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

    # }
    # dapr_data = {
    #     'test_station_01': [(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (3, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

    # }
    # dapr_data.update( {
    #     'test_station_01': [(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (3, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

    # } )


#     prcp_data = {
#             'USC00041990':	[(0, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (36, '  7'), (0, '  7'), (0, '  7'), (48, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (254, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (23, '  7'), (0, 'T 7'), (74, '  7')]

#         }
#     mdpr_data = {
# 'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (46, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }
#     dapr_data = {
# 'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }

#     prcp_data = {
#             'USC00041990':	[(0, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (36, '  7'), (0, '  7'), (0, '  7'), (48, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (254, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (23, '  7'), (0, 'T 7'), (74, '  7')]
#             # 'USC00041990':	None
#         }
#     # prcp_data = {

#     #     }

#     mdpr_data = {
# 'USC00041990':	[(100, '   '), (100, ' 77'), (-9999, '   '), (-9999, '   '), (0, '  7'), (-9999, '   '), (-9999, '   '), (50, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]
# # 'USC00041990':	None
#     }
#     dapr_data = {
# 'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (8, '   '), (60, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }


#     prcp_data = {
# 'USC00043182':	[(0, '  7'), (0, '  7'), (64, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (20, '  7'), (0, '  7'), (30, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (0, 'T 7'), (0, 'T 7'), (13, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#         }
#     mdpr_data = {
# 'USC00043182':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (114, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (58, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (51, '  7'), (-9999, '   ')]

#     }
#     dapr_data = {
# 'USC00043182':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (3, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (3, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (3, '  7'), (-9999, '   ')]

#     }

    # Update the PRCP dictionary with stations that have no PRCP data (but have MDPR and DAPR)
    


    for key in set(mdpr_data) | set(dapr_data) | set(full_station_id_list):  
        if key not in prcp_data:
            prcp_data[key] = None




    # Output the updated A
    # print(f" Updated prcp data: {prcp_data}")



    
# Daily Calculations

    daily_precip_table_rec = {}
    for station, data in prcp_data.items():
        print(station)

        pcnrec = [""] * 33 # Station Name, Precip Total, pcn record, pcn record, etc. 
    
        idy = 1     # Integer for current day
        inullct = 0 # Integer for null number of days??
        idyct = 0   # ???
        ptrace = False
        pcnFlagged = False
        total_pcn = 0
        pcn_count = 0 # each valid and non Q flagged PRCP data day
        ieommd = 0 # Still don't know what this represents.
        pcn_acc = False # Accumulated Precip flag
        pcn_missing = False
        
        pcnrec[0] = station

        for i in range(31): 
         #If there's precip data
            
            if data is not None: # each day
                # print(data)
                try:    # print(item)
                    pcn = data[i][0]
                    flg = data[i][1][:1]
                    qflg = data[i][1][1:2]
                    
                    print(f"pcn:{pcn}  flg:{flg}  qflg:{qflg}")
                except IndexError as err:
                    # Handling months without 31 days
                    # print("error: {}".format(traceback.format_exc()))
                    pcnrec[idy+1] = "  "
                    idy+=1
                    continue

                if pcn != -9999:
                    if qflg == " ":
                        d = float(pcn) * 0.1
                        # print(f"mm: {d}  in: {round_it(get_mm_to_in(d),2)}")
                        d = round_it(get_mm_to_in(d),2)
                        # pcnrec[idy+1] = round_it(d,2)
                        pcnrec[idy+1] = d
                        
                        if pcnrec[idy+1] == '0.00':
                            pcnrec[idy+1] = " "

                        total_pcn += float(d) # Add to Total PCN
                        
                        total_pcn = float(round_it(total_pcn, 2)) #Is this rounding required? 

                        pcn_count += 1
                        ieommd = 0

                        if flg == "T":
                            ptrace = True


                    else:
                        d = float(pcn) * 0.1
                        # print(f"mm: {d}  in: {round_it(get_mm_to_in(d),2)}")
                        d = get_mm_to_in(d)
                        pcnrec[idy+1] = round_it(d,2)
                        pcnFlagged = True

                    idyct += 1

                    if flg == "T":
                        pcnrec[idy+1] = "T  "

                    
                    print(f"Line: pcnrec[idy+1]:{pcnrec[idy+1]} \ttotal_pcn:{total_pcn} \tpcn_count:{pcn_count} \tieommd:{ieommd}\tptrace:{ptrace} pcnFlagged:{pcnFlagged}")
                
                
                else:  # pcn == -9999
                    try:
                        if mdpr_data[station] is not None: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            # print(mdpr_data[station])
                            pcn = mdpr_data[station][i][0]
                            flg = mdpr_data[station][i][1][:1]
                            # qflg = mdpr_data[station][i][1][2:3]
                            qflg = mdpr_data[station][i][1][1:2]
                            ndays = 0


                            # print(f"MDPR: pcn {pcn}  flg {flg}  qflg {qflg}")

                            if pcn != -9999:
                                if qflg == " ":
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                    #  DAPR
                                    try:
                                        if dapr_data[station]:
                                            # print(f"dapr station: {dapr_data[station]}")
                                            days = dapr_data[station][i][0]
                                            # print(days)
                                            # print(f"Before DAPR Calc: {pcnrec}")

                                            if days != -9999:
                                                ix = i+1 # Index for this calculation
                                                for i2 in range(1, days): # Flags the number of days indicated by DAPR data.
                                                    try:
                                                        pcnrec[ix] = "* "
                                                        ix -= 1
                                                        idyct += 1
                                                        # print(f"Pass {i2}: {pcnrec}")
                                                    except (ValueError, IndexError):
                                                        print("Too many days")

                                    except KeyError as err:
                                        pass 
                                
                                else:
                                    d = float(pcn) * 0.1
                                    d = get_mm_to_in(d)
                                    pcnrec[idy+1] = round_it(d,2) + "a"

                                if flg == "T":
                                    pcnrec[idy+1] = "Ta"
                            else:
                                pcnrec[idy+1] = "-  "

                            #  CODE here
                            try:
                                if dapr_data[station]is not None:
                                    try:
                                        ndays = dapr_data[station][i][0]
                                        print(f"ndays: {ndays}")
                                        if ndays != -9999:
                                            if i >= ndays - 1:
                                                pcn_count += ndays
                                                pcn_acc = True
                                            else:
                                                pcn_count = i + 1
                                                pcn_acc = True

                                            if pcn != -9999:
                                                if qflg == " ": 
                                                    d = float(pcn) * 0.1
                                                    d = get_mm_to_in(d)
                                                    d = float(round_it(d, 2))

                                                    total_pcn += d # Add to Total PCN
                                                    ieommd = 0
                                                else:
                                                    pcnFlagged = True
                                            else:
                                                ieommd += 1
                                                pcn_missing = True    

                                        else:
                                            print(f"ndays == -9999")
                                            ndays = 0
                                            pcn_missing = True
                                            ieommd += 1

                                    except ValueError as err:
                                        print("error: {}".format(traceback.format_exc()))
                                        pass
                                else:
                                    raise KeyError(f"{station} has DAPR key but no data.")

                            except KeyError as err:
                                print("error: {}".format(traceback.format_exc()))
                                print(f"NOTE: No DAPR data")
                                pcn_missing = True
                                ieommd += 1
                        else:
                            raise KeyError(f"{station} has MDPR key but no data.")

                            
                    except KeyError as err:
                        print("error: {}".format(traceback.format_exc()))
                        print(f"No MDPR Data")
                        pcnrec[idy+1] = "-  "
                        pcn_missing = True
                        ieommd += 1

                print(f"Line2: total_pcn: {total_pcn} pcn_count: {pcn_count} pcn_acc: {pcn_acc} pcn_missing:{pcn_missing} ieommd:{ieommd}")
            else:
                if i < num_days:
                    inullct += 1
                    pcnrec[idy+1] = "-  "

            
                    try:
                        if mdpr_data[station]is not None: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                            
                            
                            # print(mdpr_data[station])
                            # print(f"L: {len(mdpr_data[station])} MDPR station: {mdpr_data[station]}")
                            try:
                                pcn = mdpr_data[station][i][0]
                                qflg = mdpr_data[station][i][1][1:2]
                            except IndexError as err: # Handling months with less than 31 days.
                                print("error: {}".format(traceback.format_exc()))
                                pcn = None 
                                qflg = None                             
                            try:
                                ndays = dapr_data[station][i][0] if dapr_data[station] is not None else 0
                            except KeyError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = 0
                            except IndexError as err:
                                print("error: {}".format(traceback.format_exc()))
                                ndays = None

                            if (pcn or qflg or ndays) is None: 
                                print(f"Skipping because of index {i}")
                                continue
                            
                            print(f"NOTE: Else.if mdpr_data[station]: pcn {pcn} qflg {qflg} ndays {ndays}")

                            if pcn != -9999:
                                if qflg == " ":
                                    # d = 
                                    # d = get_mm_to_in(d)
                                    pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                    # total_pcn += pcn
                                    total_pcn = float(round_it(total_pcn +  pcn, 2))

                                    ndays = int(ndays)
                                    if ndays < i:
                                        print(f"Note: ndays < i = False")
                                        pcn_count += ndays if ndays != -9999 else 0
                                    else:
                                        print(f"Note: ndays < i = True")
                                        pcn_count = i
                                        pcn_acc = True

                                    if pcn != -9999:
                                        if qflg == " ":
                                            pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                            # total_pcn += pcn
                                            total_pcn = float(round_it(total_pcn +  pcn, 2))
                                            ieommd = 0
                                            print(f'NOTE: In second loop. ')
                                    else: # This clause is unreachable.
                                        ieommd += 1
                                        pcn_missing = True
                                else:
                                    pcnFlagged = True
                                    # ieommd += 1 # I feel like this should be here, but it isn't. 
                            else:
                                pcn_missing = True

                            print(f"NOTE: Else.if mdpr_data[station].inches: pcn {pcn} qflg {qflg} ndays {ndays} total_pcn {total_pcn} " 
                                    + f"\npcn_count {pcn_count} pcn_acc {pcn_acc} pcnFlagged {pcnFlagged} pcn_missing {pcn_missing} ieommd {ieommd} iteration {i}")
                        
                        else:
                            pcn_missing = True
                                
                    except KeyError as err:
                        print("error: {}".format(traceback.format_exc()))
                        pcn_missing = True





            
            idy+=1   


                # Add Flags to the Total Precip Calculation
        day_diff = monthrange(year, month)[1] - pcn_count

        if day_diff == 0:
            pcn_missing = False
        
        setAstr = False # Set Asterisk
        still_missing = True

        if day_diff <= 9:
            flag_total_pcn = round_it(total_pcn, 2)

            if day_diff == ieommd and ieommd > 0:
                still_missing = check_next_month_for_acc_pcn(station, month, year, ieommd)
                if not still_missing:
                    pcn_missing = False
                    setAstr = True

            print(f"NOTE: flag_total_pcn={flag_total_pcn} ptrace={ptrace} day_diff={day_diff} pcn_missing={pcn_missing} setAstr={setAstr} still_missing={still_missing}" )

            if ptrace and flag_total_pcn == "0.00":
                flag_total_pcn = "T"

            label = ""


            # TESTING FLAG LOGIC
            # label = True
            # setAstr = True

            #####
            if pcn_acc:
                if pcn_missing:
                    label = "FMA" if pcnFlagged else "MA"
                else:
                    label = "FA" if pcnFlagged else "A"
            else:
                if pcn_missing:
                    label = "FM" if pcnFlagged else "M"
                else:
                    label = "F" if pcnFlagged else ""

            if label:
                if setAstr:
                    flag_total_pcn = f"{label}* {flag_total_pcn}"
                else:
                    flag_total_pcn = f"{label} {flag_total_pcn}"
            elif setAstr:
                flag_total_pcn = f"* {flag_total_pcn}"
        
        else:
            flag_total_pcn = "M"


        print(f"pcn_acc={pcn_acc} pcn_missing={pcn_missing} label={label} setAstr={setAstr}")
                    
        print(f"NOTE: flag_total_pcn={flag_total_pcn} ptrace={ptrace} day_diff={day_diff} pcn_missing={pcn_missing} setAstr={setAstr} still_missing={still_missing}" )
        
        station_name = None
        print(f"station: {station} total precip = {total_pcn} flag_total_pcn={flag_total_pcn}")
        if station in load_station_data():
            station_name = load_station_data()[station][1]
        # print(f"station={station} {station_name}: idy={idy}, inullct={inullct}, idyct={idyct}, ptrace={ptrace}, pcnFlagged={pcnFlagged}, "
        #     f"total_pcn={total_pcn}, pcn_count={pcn_count}, ieommd={ieommd}, "
        #     f"pcn_acc={pcn_acc}, pcn_missing={pcn_missing}")
        
        write_to_file(f"station: {station} {station_name}: {total_pcn} flag_total_pcn={flag_total_pcn}", "tprcp_output.txt")


        # Add to dictionary
        pcnrec[1] = flag_total_pcn
        print(pcnrec)

    
        write_to_file(
            f"station={str(station):<13}  {str(station_name):<40}"
            f"  flag_total_pcn={str(flag_total_pcn):<5}"
            f"  total_pcn={str(total_pcn):<5}"
            f"  idy={str(idy):<3}"
            f"  inullct={str(inullct):<2}"
            f"  idyct={str(idyct):<3}"
            f"  ptrace={str(ptrace):<5}"
            f"  pcnFlagged={str(pcnFlagged):<5}"
            f"  pcn_count={str(pcn_count):<2}"
            f"  ieommd={str(ieommd):<1}"
            f"  pcn_acc={str(pcn_acc):<5}"
            f"  pcn_missing={str(pcn_missing):<5}"
            f"\n{str(station_name)} {str(pcnrec)}"
        )

        ##############################
        # Printing the results for each station to a file to QA them. 
        result = {}
        result['station_id'] = pcnrec[0]
        result['tprcp'] = pcnrec[1].strip()

        for i, value in enumerate(pcnrec[2:], start=1):
            label = f"{i:02d}"
            result[label] = value.strip()

        write_to_file(
             f"{str(station_name)} {str(result)}\n"
             , "tprcp_output.txt"
        )
        ############################
        
        # End result format
        result = {}
        # result['station_id'] = pcnrec[0]
        result['total_pcn'] = pcnrec[1].strip()

        daily_pcn = {}
        for i, value in enumerate(pcnrec[2:], start=1):
            label = f"{i:02d}"
            daily_pcn[label] = value.strip()

        # result.update(daily_pcn)
        result["daily_pcn"] = daily_pcn
        print(f"daily: {daily_pcn} result: {result}")

        daily_precip_table_rec.setdefault(station, {}).update(result)



    return daily_precip_table_rec




def check_next_month_for_acc_pcn(station_id: str, month: int,year: int, ieommd: int) -> bool:
    """ Checks the next month for accumulated precipitation.

    Parameters
    ----------
    station_id : str
        GHCN-ID
    month, year : int

    ieommd : int
        Honestly I don't know what this stands for. I tried. 

    Returns
    -------
    bool
    """
    all_filtered_dfs = []
    noDataCount = 0

    still_missing = True

    # Parse month and increment
    month = month
    year = year

    if month < 12:
        month += 1
    else:
        month = 1
        year += 1

    ######################
    # Get next month's data

    file_path = f"/data/ops/ghcnd/data/ghcnd_all/{station_id}.dly"

    if not os.path.exists(file_path):
        print(f"Missing file: {file_path}")
        return still_missing

    try:
        filtered_data = parse_and_filter(
            station_code=station_id,
            file_path=file_path,
            correction_type="table",
            month=month,
            year=year
        )

        filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

        if filtered_df.is_empty():
            print(f"Skipping station {station_id} due to no data.")
            noDataCount += 1
            return still_missing

        if all_filtered_dfs:
            existing_columns = all_filtered_dfs[0].columns
            current_columns = filtered_df.columns

            missing_columns = set(existing_columns) - set(current_columns)
            for col in missing_columns:
                filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

            filtered_df = filtered_df.select(existing_columns)

        all_filtered_dfs.append(filtered_df)
        print(f"Parsed {len(filtered_df)} records from {station_id}")

    except Exception as e:
        print(f"Error parsing {station_id}: {e}")
        return still_missing

    if not all_filtered_dfs:
        print("No valid station files found.")
        return still_missing

    combined_df = pl.concat(all_filtered_dfs, how="vertical")

    json_data = json.dumps(combined_df.to_dicts(), indent=2)
    json_data = json.loads(json_data)

    # # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)

    year = json_data[0]["year"]
    month = json_data[0]["month"]
    num_days = monthrange(year, month)[1]
    
    prcp_data = {}
    mdpr_data = {}
    dapr_data = {}

    # Collect Precip data
    for row in json_data:
        station_id = f"{row['country_code']}{row['network_code']}{row['station_code']}"
        obs_type = row["observation_type"]

        # List Comprehesion
        daily_values = [
            int(row[f"day_{i}"]) if row[f"day_{i}"] is not None else -9999
            for i in range(1, num_days + 1)    
        ]
        daily_flags = [
            row[f"flag_{i}"] # if row[f"day_{i}"] is not None else None
            for i in range(1, num_days + 1)
        ]

        if obs_type == "PRCP":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            prcp_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "MDPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            mdpr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))
        elif obs_type == "DAPR":
            # prcp_data.setdefault(station_id, []).extend(daily_values)
            dapr_data.setdefault(station_id, []).extend(list(zip(daily_values, daily_flags)))

    # for key, data in prcp_data.items():
    #     # print(f"{key}:\t{data}")
    #     write_to_file(f"{key}:\t{data}", "program_output2.txt")
    # write_to_file(f"-"*30, "program_output2.txt")
    # for key, data in mdpr_data.items():
    #     # print(f"{key}:\t{data}")
        
    #     write_to_file(f"{key}:\t{data}", "program_output2.txt")
    #     write_to_file(len(data), "program_output2.txt")
    # write_to_file(f"-"*30, "program_output2.txt")
    # for key, data in dapr_data.items():
    #     # print(f"{key}:\t{data}")
        
    #     write_to_file(f"{key}:\t{data}", "program_output2.txt")
    #     write_to_file(len(data), "program_output2.txt")


    #########################
# OG Unedited data
#     prcp_data = {
# "USC00040383":	[(292, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (356, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (300, '  7'), (41, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (66, '  7'), (61, '  7'), (64, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#         }
#     mdpr_data = {
# "USC00040383":	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (168, '  7'), (-9999, '   '), (114, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (343, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (244, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }
#     dapr_data = {
# "USC00040383":	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }

# Testing Data (Edited)
#     prcp_data = {
# "USC00040383":	[(-9999, '  7'), (9999, '  7'), (9999, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (9999, '  7'), (356, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (300, '  7'), (41, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (66, '  7'), (61, '  7'), (64, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#         }
#     mdpr_data = {
# "USC00040383":	[(10, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (168, '  7'), (-9999, '   '), (114, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (343, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (244, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }
#     dapr_data = {
# "USC00040383":	[(1, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }


    for i in range(32):

        try:
            try:
                pcn = prcp_data[station_id][i][0] if prcp_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn = None
            
            try:
                pcn_mdpr = mdpr_data[station_id][i][0] if mdpr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                pcn_mdpr = -9999
            
            try:
                days = dapr_data[station_id][i][0] if dapr_data[station_id][i][0] is not None else None
            except KeyError as err:
                print("error: {}".format(traceback.format_exc()))
                days = None
            

        except IndexError as err:
            print(f"Breaking from next month acc loop.")
            break # Exit early if data is shorter than expected
    
        print(f"pcn={pcn} pcn_mdpr={pcn_mdpr} days={days}")

        # days = None
        if pcn_mdpr != -9999 and days != -9999:
            try:
                days = int(days)
                day_diff = days - (i + 1)
                print(day_diff)
                if day_diff == ieommd:
                    still_missing = False
                
            except TypeError:
                pass   
            break # Exit loop if accumulated value found

        if pcn != -9999:
            break # Exit loop if direct value found

    return still_missing

#############################################################################

def round_it(d: float, dec_place: int) -> str:
    val = ""

    if dec_place == 0:
        if d >= 0:
            val = str(d + 0.50000001)
            ix = val.find(".")
            val = val[:ix]
        else:
            val = str(d - 0.50000001)
            ix = val.find(".")
            if ix != -1:
                val = val[:ix]
                if val == "-0":
                    val = "0"

    elif dec_place == 1:
        if d >= 0:
            val = str(d + 0.050000001)
            ix = val.find(".")
            val = val[:ix+2]
        else:
            val = str(d - 0.050000001)
            ix = val.find(".")
            val = val[:ix+2]
            if val == "-0.0":
                val = "0.0"

    elif dec_place == 2:
        if d >= 0:
            val = str(d + 0.0050000001)
            ix = val.find(".")
            val = val[:ix+3]
        else:
            val = str(d - 0.0050000001)
            ix = val.find(".")
            val = val[:ix+3]
            if val == "-0.00":
                val = "0.00"

    return val

def write_to_file(obj, filename="program_output_199705.txt", path="temp/daily_precip/"):
    filename = os.path.join(path, filename)
    if not hasattr(write_to_file, "write_flags"):
        write_to_file.write_flags = {}

    if filename not in write_to_file.write_flags:
        mode = 'w'
        write_to_file.write_flags[filename] = True
    else:
        mode = 'a'

    with open(filename, mode, encoding='utf-8') as f:
        f.write(str(obj) + '\n')

def load_station_data( filename = os.path.join("/data/ops/onyx.imeh/datzilla-flask/", "ghcnd-stations.txt")):
    """Load station data into a dictionary keyed by station ID."""
    stations = {}
    with open(filename, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue  # skip malformed lines
            station_id = parts[0]
            state = parts[4]
            name = " ".join(parts[5:])  # everything after the state is the name
            stations[station_id] = (state, name)
    return stations

if __name__ == "__main__":
    
    results = generateDailyPrecip()
    
    
    print("*"* 30)
    # Print all
    print(results)

    # Print one item at a time
    for k, v in results.items():
        print(f"{k} {v}")
    
    # Print recursively
    # for k, v in results.items():
    #     print(k, end="")
    #     if type(v) is dict:
    #         print("\n")
    #         for k2, v2 in v.items():
    #             print(k2, end="")
    #             if type(v2) is dict: 
    #                 print("\n")
    #                 for k3, v3 in v2.items():
    #                     print(f"{k3} : {v3}")
    #             else:
    #                 print(f": {v2}")
    #     else:
    #         print(v)

    
    # Write to file for Testing
    write_to_file("*"* 30)
    for key, value  in results.items():
        write_to_file(f"{key}:  {value}")

        



    # print(check_next_month_for_acc_pcn("USC00040383", 2, 2023, 0))
