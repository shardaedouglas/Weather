
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

# print(f"{record:<20} {str(float(record) >= .01):<8} {str(float(record) >= .10):<8} {str(float(record) >= 1.00):<8}")

# Testing Multiline queries
# stations = QuerySoM("som")
# print(bool(stations))
# for station in stations[:20]:
#     print(station)

#Testing Daily Precip Query

def get_mm_to_in(mm: float) -> float:
    """Convert millimeters to inches."""
    return mm * 0.03937 # 1 inch = 25.4 mm


def generateDailyPrecip():
    month = 2
    year = 2023
    
    # # Get Station List
    # stations = QueryDB(DailyPrecipQuery)
    # print(bool(stations))
    # for station in stations[-20:]:
    #     print(station)

    # all_filtered_dfs = []
    # noDataCount = 0

    # for row in stations[:20]:
    #     ghcn_id = row[4]
    #     file_path = f"/data/ops/ghcnd/data/ghcnd_all/{ghcn_id}.dly"

    #     if not os.path.exists(file_path):
    #         print(f"Missing file: {file_path}")
    #         continue

    #     try:
    #         filtered_data = parse_and_filter(
    #             station_code=ghcn_id,
    #             file_path=file_path,
    #             correction_type="table",
    #             month=month,
    #             year=year
    #         )

    #         filtered_df = pl.DataFrame(filtered_data) if isinstance(filtered_data, dict) else filtered_data

    #         if filtered_df.is_empty():
    #             print(f"Skipping station {ghcn_id} due to no data.")
    #             noDataCount += 1
    #             continue

    #         if all_filtered_dfs:
    #             existing_columns = all_filtered_dfs[0].columns
    #             current_columns = filtered_df.columns

    #             missing_columns = set(existing_columns) - set(current_columns)
    #             for col in missing_columns:
    #                 filtered_df = filtered_df.with_columns(pl.lit(None).alias(col))

    #             filtered_df = filtered_df.select(existing_columns)

    #         all_filtered_dfs.append(filtered_df)
    #         print(f"Parsed {len(filtered_df)} records from {ghcn_id}")

    #     except Exception as e:
    #         print(f"Error parsing {ghcn_id}: {e}")
    #         continue

    # if not all_filtered_dfs:
    #     print("No valid station files found.")
    #     return

    # combined_df = pl.concat(all_filtered_dfs, how="vertical")

    # json_data = json.dumps(combined_df.to_dicts(), indent=2)

    # # Optional: write JSON string to file
    # output_file = f"combined_data_{month}_{year}.json"
    # with open(output_file, "w") as f:
    #     f.write(json_data)
    
    
    
    
    # print(f"Data saved to {output_file}")

    # print(json_data)


    #####################################################
    #  Read the JSON file. 


    json_data = None

    with open('combined_data_2_2023.json') as f:
        json_data = json.load(f)
        # print(d)


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


#     prcp_data = {
#             'USC00041990':	[(0, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (36, '  7'), (0, '  7'), (0, '  7'), (48, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (254, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (23, '  7'), (0, 'T 7'), (74, '  7')]

#         }
#     mdpr_data = {
# 'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (46, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }
#     dapr_data = {
# 'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

#     }

    prcp_data = {
            # 'USC00041990':	[(0, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (36, '  7'), (0, '  7'), (0, '  7'), (48, '  7'), (0, '  7'), (0, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (0, '  7'), (0, '  7'), (254, '  7'), (-9999, '   '), (0, '  7'), (0, '  7'), (23, '  7'), (0, 'T 7'), (74, '  7')]
            'USC00041990':	None
        }
    prcp_data = {

        }

    mdpr_data = {
'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (46, '  7'), (-9999, '   '), (-9999, '   '), (50, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

    }
    dapr_data = {
'USC00041990':	[(-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (2, '  7'), (-9999, '   '), (8, '   '), (60, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   '), (-9999, '   ')]

    }


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
    for key in set(mdpr_data) | set(dapr_data):  
        if key not in prcp_data:
            prcp_data[key] = None

    # Output the updated A
    print(f" Updated prcp data: {prcp_data}")



    
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
                        if mdpr_data[station]: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
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
                                if dapr_data[station]:
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
                            except KeyError as err:
                                print("error: {}".format(traceback.format_exc()))
                                print(f"NOTE: No DAPR data")
                                pcn_missing = True
                                ieommd += 1
                        

                            
                    except KeyError as err:
                        print("error: {}".format(traceback.format_exc()))
                        print(f"No MDPR Data")
                        pcnrec[idy+1] = "-  "
                        pcn_missing = True
                        ieommd += 1

                print(f"Line2: total_pcn: {total_pcn} pcn_count: {pcn_count} pcn_acc: {pcn_acc} pcn_missing:{pcn_missing} ieommd:{ieommd}")
            else:
                inullct += 1
                pcnrec[idy+1] = "-  "

                #  CODE HERE
                try:
                    if mdpr_data[station]: # Check MDPR (Number of days with non-zero precipitation included in multiday precipitation total)
                        
                        
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

                        if pcn is not -9999:
                            if qflg == " ":
                                # d = 
                                # d = get_mm_to_in(d)
                                pcn = float(round_it(get_mm_to_in(pcn * 0.1), 2))
                                total_pcn += pcn

                                ndays = int(ndays)
                                if ndays < i:
                                    print(f"Note: ndays < i = False")
                                    pcn_count += ndays
                                else:
                                    print(f"Note: ndays < i = True")
                                    pcn_count = i
                                    pcn_acc = True

                        print(f"NOTE: Else.if mdpr_data[station].inches: pcn {pcn} qflg {qflg} ndays {ndays} total_pcn {total_pcn} " 
                                + f"\npcn_count {pcn_count} pcn_acc {pcn_acc} iteration {i}")


                            
                except KeyError as err:
                    print("error: {}".format(traceback.format_exc()))
                    pcn_missing = True

            
            
            idy+=1   

            #  31 day loop level
            # Total Precipitation Calculation


        # Add to dictionary
        # print(pcnrec)


        daily_precip_table_rec.setdefault(station, []).extend(pcnrec)


      


    return daily_precip_table_rec






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

def write_to_file(obj, filename="program_output2.txt"):
    if not hasattr(write_to_file, "has_written"):
        mode = 'w'
        write_to_file.has_written = True
    else:
        mode = 'a'

    with open(filename, mode, encoding='utf-8') as f:
        f.write(str(obj) + '\n')


if __name__ == "__main__":
    results = generateDailyPrecip()
    # write_to_file("*"* 30)
    # print(results)
    write_to_file("*"* 30)
    for key, value  in results.items():
        write_to_file(f"{key}:  {value}")
        write_to_file(f"{len(value)}")
        
        # for i, value in enumerate(generateDailyPrecip(), start=-1):
    #     # print(f"{i}: {value} {type(value)}")
    #     print(f"{i}: {value}")
