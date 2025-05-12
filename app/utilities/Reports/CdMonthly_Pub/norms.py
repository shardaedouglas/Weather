import os
import traceback

# Temporary File for Norms functions.

# NOTE: replace these with external functions or variables. 
CoopToGhcn_defaultPath = "/" + os.path.join("data", "ops", "ghcndqi")
imo = 2
hddid=[]
hddval=[]



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




###########################################################################################


def get8110shdd(gid: str):
    """
    get8110shdd - get ??? for 1981 - 2010
        @param gid - GHCN Daily ID
        @return str
    """
    dat = ""

    try:
        # Actual Path to use
        fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        # script_dir = os.path.dirname(os.path.abspath(__file__))
        # fn = os.path.join(script_dir, gid + ".normals.txt")

        with open(fn, "r") as file:
            for line in file: 
                if (line is not None):
                    if (len(line) > 29):
                        elem = line[0:23]
                        if ("        ann-htdd-normal" in elem):
                            dat = line[24:29]

                    


    except Exception as err:
        print("{} {}".format(err, gid))

    return dat





def computeDivDFN( id: str,  atmp: str, pcn: str,  mo: str):
    """
    computeDivDFN - Compute Divisioanl DFN.
  		@param id - 4 character string
  		@param atmp
  		@param pcn
  		@param mo
  		@return dfn - list[str]
    """
    dfn = [None,None]

    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641F_1971-2000-NORM_CLIM85.txt")
    line2 = None
    line3 = None

    try:
        # print(fn)
        with open(fn, "r") as file:
            for line in file:
                # print(line)
                if (line is not None):

		# 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
		# 1010119712000  395   438   517   595   677   752   789   778   717   605   507   426    600 2   

                    tid = line[1:5]
                    tt1 = line[0:1]
                    tt2 = line[92:93]

                    if (tid == id):
                        if (tt1 == "1"):
                            if (tt2 == "2"): # Av Temp
                                line2 = line

                        elif (tt1 == "2"): # Precip
                            if (tt2 == "2"):
                                line3 = line
                                break
                else: 
                    break


        if (line2 is not None):
            dfm = []

            dfm.append(line2[13:18])
            dfm.append(line2[19:24])
            dfm.append(line2[25:30])
            dfm.append(line2[30:36])
            dfm.append(line2[37:42])
            dfm.append(line2[43:48])
            dfm.append(line2[49:54])
            dfm.append(line2[55:60])
            dfm.append(line2[61:66])
            dfm.append(line2[67:72])
            dfm.append(line2[73:78])
            dfm.append(line2[79:84])

        

            dfn[0] = dfm[imo-1]



            try:
                ix = atmp.find("M")
                if (ix > -1):
                    atmp = atmp[0:ix]
                

                d1 = float(atmp)
                d2 = float(dfn[0])
                d2 = d2 * 0.1


                d3 = d1 - d2
                dfn[0] = round_it(d3, 1)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(err))
                dfn[0] = " "
              


        else: 
            dfn[0] = " "


        
        if (line3 is not None):
            dfm = []

            dfm.append(line3[13:18])
            dfm.append(line3[19:24])
            dfm.append(line3[25:30])
            dfm.append(line3[30:36])
            dfm.append(line3[37:42])
            dfm.append(line3[43:48])
            dfm.append(line3[49:54])
            dfm.append(line3[55:60])
            dfm.append(line3[61:66])
            dfm.append(line3[67:72])
            dfm.append(line3[73:78])
            dfm.append(line3[79:84])

        


            dfn[1] = dfm[imo-1];

 

            try:
                # ix = atmp.index("M")
                # print(ix)
                # if (ix > -1):
                #     atmp = atmp[:ix]
                

                d1 = float(pcn)
                d2 = float(dfn[1])
                d2 = d2 * 0.01



                d3 = d1 - d2
                dfn[1] = round_it(d3, 2)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(traceback.format_exc()))
                dfn[1] = " "
              



        else: 
            dfn[1] = " "

    except Exception as err: 
        print("error: {}".format(traceback.format_exc()))
    
    return dfn


def loadHddNorm():
    """
    loadHddNorm -  Get Hdd Norms.
    Loads HDD Norms to variables hddval and hddid
    """
    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641C_1971-2000_NORM_CLIM81_MTH_STNNORM")

    try:
        with open(fn, "r") as file:
            for line in file:
                if line is not None:
                    tt1 = line[6:9]
                    # print(line)
                    # print(tt1)
                    if (tt1 == "604"): # HDD
                        # //      1         2         3         4         5         6         7         8         9								
                        # //0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
                        # //014064604   780    587    404    180     41      1      0      0     18    165    417    669    3262
                        tid = line[:6]
                        hddid.append(tid)

                        hdd = line[95:100]
                        hddval.append(hdd)


    
    except Exception as err: 
        print("error: {}".format(traceback.format_exc()))




# NOTE: This should be in ghcnDataBrowser.java
def getMlyNormals8110(gid: str):
    """
    getMlyNormals8110 - Get monthly 81-2010 normals.
        @param gid  - GHCND ID.
        @return list[str]  
    """
    
    tmax = ""
    tmin = ""
    tavg = ""

    cldd = ""
    hldd = ""
    prcp = ""
    snow = ""
    ok = False


    try:
        fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        with open(fn, "r") as file: 
            for line in file: 
                if line is not None:
                    try:
                        header = line[:23]
                        
                        if (header == "        mly-tmax-normal"):
                            tmax = line[23:]
                            ok = True
                        elif (header == "        mly-tavg-normal"):
                            tavg = line[23:]
                        
                        elif (header == "        mly-tmin-normal"):
                            tmin = line[23:]
                        elif (header == "        mly-cldd-normal"):
                            cldd = line[23:]
                        elif (header == "        mly-htdd-normal"):
                            hldd = line[23:]
                        elif (header == "        mly-prcp-normal"):
                            prcp = line[23:]
                        elif (header == "        mly-snow-normal"):
                            snow = line[23:]
                        
                    except Exception as err:
                        print("error: {}".format(traceback.format_exc()))
   

    except Exception as err:
        ok = False

    
# 		/*
#           1         2         3         4         5         6         7         8          
# 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
#    464R   491R   586R   694R   794R   888R   921R   899R   825R   714R   605R   497R
#    318R   341R   401R   508R   609R   707R   750R   731R   653R   541R   436R   348R
# 		 */


    dat = [''] * 7


    if ok:
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            try:
                val = tmax[i1:i1+6]
                d = float(val)
                d *= 0.1

                if (i == 0):
                    rec = round_it(d, 1)
                else:
                    rec = rec+","+round_it(d,1)

            except Exception as err:
                print("{} temp missing".format(gid))
                break
            
            i1 += 7
        
        dat[0] = rec

        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tmin[i1:i1+6]
            d = float(val)
            d *= 0.1


            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec += ","+round_it(d,1)


            i1 += 7
        

        dat[1] = rec


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tavg[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec += ","+round_it(d,1)
            
            i1 += 7
        

        dat[2] = rec
        
        


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = hldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7
        

        # dat[3]
        dat[3] = rec


        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = cldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7

        # dat[4]
        dat[4] = rec



        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = prcp[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.01

                    if (i == 0):
                        rec = round_it(d, 2)
                    else:
                        rec += ","+round_it(d,2)
                
                else:
                    if (i == 0):
                        rec = "0.00"
                    else:
                        rec += ",0.00"

            except Exception as err:
                if (i == 0):
                    rec = "null"
                else:
                    rec += ",null"
            
            
            i1 += 7

        # dat[5]
        dat[5] = rec
        

        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = snow[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.1

                    if (i == 0):
                        rec = round_it(d, 1)
                    else:
                        rec += ","+round_it(d,1)
                
                else:
                    if (i == 0):
                        rec = "0.0"
                    else:
                        rec += ",0.0"

            except Exception as err:
                if (i == 0):
                    rec = "0.0"
                else:
                    rec += ",0.0"
            
            
            i1 += 7

        # dat[6]
        dat[6] = rec
    
    return dat
            


# NOTE: This should be in ghcnDataBrowser.java
def getMlyNormals9121(gid: str):
    """
    getMlyNormals9121 - Get monthly 91-2020 normals.
        @param gid  - GHCND ID.
		@return list[str] - 
    """
    tmax = ""
    tmin = ""
    tavg = ""

    cldd = ""
    hldd = ""
    prcp = ""
    snow = ""

    try:
        # fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        #  TESTING
        script_dir = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(script_dir, gid + ".normals.txt")

        with open(fn, "r") as file: 
            for line in file: 
                if line is not None:
                    try:
                        header = line[:23]
                        
                        if (header == "        mly-tmax-normal"):
                            tmax = line[23:]
                            ok = True
                        elif (header == "        mly-tavg-normal"):
                            tavg = line[23:]
                        
                        elif (header == "        mly-tmin-normal"):
                            tmin = line[23:]
                        elif (header == "        mly-cldd-normal"):
                            cldd = line[23:]
                        elif (header == "        mly-htdd-normal"):
                            hldd = line[23:]
                        elif (header == "        mly-prcp-normal"):
                            prcp = line[23:]
                        elif (header == "        mly-snow-normal"):
                            snow = line[23:]
                        
                    except Exception as err:
                        print("error: {}".format(traceback.format_exc()))


    except Exception as err:
        print("error: {}".format(traceback.format_exc()))

    # 			/*
	#           1         2         3         4         5         6         7         8          
	# 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
	#    464R   491R   586R   694R   794R   888R   921R   899R   825R   714R   605R   497R
	#    318R   341R   401R   508R   609R   707R   750R   731R   653R   541R   436R   348R
	# 		 */

    dat = [''] * 7

    if (not tmax == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            try:
                val = tmax[i1:i1+6]
                d = float(val)
                d *= 0.1

                if (i == 0):
                    rec = round_it(d, 1)
                else:
                    rec = rec+","+round_it(d,1)

                
            except Exception as err:
                print("error: {}".format(traceback.format_exc()))
                break

            i1 += 7
        
        dat[0] = rec


    if (not tmin == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tmin[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec = rec+","+round_it(d,1)

            i1 += 7
        # dat[1]
        dat[1] = rec

    if (not tavg == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = tavg[i1:i1+6]
            d = float(val)
            d *= 0.1

            if (i == 0):
                rec = round_it(d, 1)
            else:
                rec = rec+","+round_it(d,1)

            i1 += 7
    
        # dat[2]
        dat[2] = rec


    if (not hldd == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            val = hldd[i1:i1+6]


            if (val == " -7777"):
                if (i == 0):
                    rec = "0"
                else:
                    rec += "," + str(0)
            else:
                if (i == 0):
                    rec = val.strip()
                else:
                    rec += "," + val.strip()

            
            i1 += 7

        # dat[3]
        dat[3] = rec



    if (not cldd == ""):
        i1 = 0
        rec = ""

        try:
            for i in range(0, 12, 1):
                val = cldd[i1:i1+6]

                if (val == " -7777"):
                    if (i == 0):
                        rec = "0"
                    else:
                        rec += "," + str(0)
                else:
                    if (i == 0):
                        rec = val.strip()
                    else:
                        rec += "," + val.strip()

            
                i1 += 7
        except Exception as err:
            print("error: {}".format(traceback.format_exc()))

        dat[4] = rec


    if (not prcp == ""):
        i1 = 0
        rec = ""
        for i in range(0, 12, 1):
            
            try:
                val = prcp[i1:i1+6]
                if ( not val == " -7777"):
                    d = float(val)
                    d *= 0.01

                    if (i == 0):
                        rec = round_it(d, 2)
                    else:
                        rec += ","+round_it(d,2)
                
                else:
                    if (i == 0):
                        rec = "0.00"
                    else:
                        rec += ",0.00"

            except Exception as err:
                if (i == 0):
                    rec = "null"
                else:
                    rec += ",null"
            
            
            i1 += 7

        # dat[5]
        dat[5] = rec

        if (not snow == ""):
            i1 = 0
            rec = ""
            for i in range(0, 12, 1):
                
                try:
                    val = snow[i1:i1+6]
                    if ( not val == " -7777"):
                        d = float(val)
                        d *= 0.1

                        if (i == 0):
                            rec = round_it(d, 1)
                        else:
                            rec += ","+round_it(d,1)
                    
                    else:
                        if (i == 0):
                            rec = "0.0"
                        else:
                            rec += ",0.0"

                except Exception as err:
                    if (i == 0):
                        rec = "0.0"
                    else:
                        rec += ",0.0"
                
                
                i1 += 7

            # dat[6]
            dat[6] = rec

    # print("{}".format(dat))
    return dat

    



def getTempNorm8110(id: str, atmp: str, pcn: str, imo: int):
    """
    getTempNorm8110 - Get temp 71 -2000 Norms. Departure from Normal for Temp and Precip.
     @param id - ghcnd id
     @param atmp -  avg Temp
     @param pcn - Precip total
     @param iMo - Month, integer. Note this is 0 indexed, not 1
     Return - List[str] - dfn
    """
    # I'm not sure what format pcn comes in as.
    dfn = [''] * 2

    # TODO: Update with the external class
    mdfn = getMlyNormals8110(id)
    # mdfn = ghcnDataBrowser.getMlyNormals8110(id)

    trec = mdfn[2]

    if (trec):
        mt = trec.split(",")
        ntmp = mt[imo]

        # multiprint(mt=mt,ntmp=ntmp)

        if (atmp.find("M") >= 0):
            atmp= atmp[0:atmp.find("M")]

        try:
            d1 = float(atmp)
            d2 = float(ntmp)

            d3 = d1- d2

            dfn[0] = round_it(d3, 1)
        except Exception as err: #TODO: Consider traceback.format_exc()
            # print("error: {}".format(traceback.format_exc()))
            dfn[0] = " "

        # multiprint(atmp=atmp, d1=d1, d2=d2, d3=d3, dfn=dfn[0])

    else:
        dfn[0] = " "

    # multiprint(dfn=dfn[0])

    prec = mdfn[5]

    if (prec):
        mt = prec.split(",")
        npcn = mt[imo]


        if( pcn.find("M") >= 0):
            pcn = pcn[:pcn.find("M")+1]
        if( pcn.find("F") >= 0):
            pcn = pcn[:pcn.find("F")+1]
        if( pcn.find("A") >= 0):
            pcn = pcn[:pcn.find("A")+1]



        try:
            d1 = float(pcn)
            d2 = float(npcn)

            d3 = d1- d2

            dfn[1] = round_it(d3, 2)
        except Exception as err: #TODO: Consider traceback.format_exc()
            print("error: {}".format(traceback.format_exc()))
            dfn[1] = " "

        # multiprint(pcn=pcn, d1=d1, d2=d2, d3=d3, dfn=dfn[1])


        # multiprint(mt=mt, npcn=npcn, pcn=pcn)

    else:
        dfn[1] = " "

    return dfn




def getTempNorm9120(id: str, atmp: str, pcn: str, imo: int):
    """
	   getTempNorm9121 - Get temp 91 -2021 Norms.
	     @param id - ghcnd id
	     @param atmp -  avg Temp
	     @param pcn - Precip total
	     @param iMo - Month, integer.

         @return list[str] - dfn
    """
    # # I'm not sure what format pcn comes in as.
    dfn = [''] * 2

    # # TODO: Update with the external class
    mdfn = getMlyNormals9121(id)
    # mdfn = ghcnDataBrowser.getMlyNormals8110(id)

    trec = mdfn[2]
    # trec = ""

    if (trec):
        mt = trec.split(",")
        ntmp = mt[imo]

        # multiprint(mt=mt,ntmp=ntmp)

        if (atmp.find("M") >= 0):
            atmp= atmp[:atmp.find("M")]

        try:
            d1 = float(atmp)
            d2 = float(ntmp)

            d3 = d1- d2

            dfn[0] = round_it(d3, 1)
        except Exception as err: #TODO: Consider traceback.format_exc()
            # print("error: {}".format(traceback.format_exc()))
            dfn[0] = " "

        # multiprint(atmp=atmp, d1=d1, d2=d2, d3=d3, dfn=dfn[0])

    else:
        dfn[0] = " "

    # multiprint(dfn=dfn[0])

    prec = mdfn[5]

    if (prec):
        mt = prec.split(",")
        npcn = mt[imo]


        if( pcn.find("M") >= 0):
            pcn = pcn[:pcn.find("M")+1]
        if( pcn.find("F") >= 0):
            pcn = pcn[:pcn.find("F")+1]
        if( pcn.find("A") >= 0):
            pcn = pcn[:pcn.find("A")+1]



        try:
            d1 = float(pcn)
            d2 = float(npcn)

            d3 = d1- d2

            dfn[1] = round_it(d3, 2)
        except Exception as err: #TODO: Consider traceback.format_exc()
    #         print("error: {}".format(traceback.format_exc()))
            dfn[1] = " "

        # multiprint(pcn=pcn, d1=d1, d2=d2, d3=d3, dfn=dfn[1])


        # multiprint(mt=mt, npcn=npcn, pcn=pcn)

    else:
        dfn[1] = " "

    return dfn






def getTempNorm7100(id: str, atmp: str, pcn:str, imo: int):
    """
    getTempNorm7100 -  Get temp 71 -2000 Norms.
	  @param id - coop id
	  @param atmp -  avg Temp
      @param pcn - Precip total
      @param iMo - Month, integer. 

      @return - list[str] - departure from normal
    
    """
    
    dfn = [''] * 2

    fn = os.path.join(CoopToGhcn_defaultPath, "norms", "9641C_1971-2000_NORM_CLIM81_MTH_STNNORM")

    line2 = ""
    line3 = ""

    try:
        with open(fn, "r") as file:
            for line in file:
                if (line):
                    
                    tid = line[:6]
                    tt1 = line[6:7]
                    tt2 = line[7:9]

                    if (tid == id):
                        if (tt1 == "3"):
                            if (tt2 == "04"): # Av Temp
                                line2 = line

                        elif (tt1 == "4"): # Precip
                            if (tt2 == "04"):
                                line3 = line
                                break
                    
                else: 
                    break
    
            

    



        if (line2):
            dfm = [''] * 12

            dfm[0]=line2[9:15]
            dfm[1]=line2[15:22]
            dfm[2]=line2[22:29]
            dfm[3]=line2[29:36]
            dfm[4]=line2[36:43]
            dfm[5]=line2[43:50]
            dfm[6]=line2[50:57]
            dfm[7]=line2[57:64]
            dfm[8]=line2[64:71]
            dfm[9]=line2[71:78]
            dfm[10]=line2[78:85]
            dfm[11]=line2[85:92]

            dfn[0] = dfm[imo]

            try:
                ix = atmp.find("M")
                if (ix > -1):
                    atmp = atmp[0:ix]
                ix = atmp.find("F")
                if (ix > -1):
                    atmp = atmp[0:ix]
                

                d1 = float(atmp)
                d2 = float(dfn[0])
                d2 = d2 * 0.1


                d3 = d1 - d2
                dfn[0] = round_it(d3, 1)

                # multiprint(atmp=atmp, d1=d1 ,d2=d2 ,d3=d3 , dfn_0=dfn[0])

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                dfn[0] = " "
        else: 
                dfn[0] = " "    

        # multiprint(dfn_0=dfn[0], atmp=atmp, line3=line3)    

        # line3= None
        if (line3):
            dfm = [''] * 12

            dfm[0]=line2[9:15]
            dfm[1]=line2[15:22]
            dfm[2]=line2[22:29]
            dfm[3]=line2[29:36]
            dfm[4]=line2[36:43]
            dfm[5]=line2[43:50]
            dfm[6]=line2[50:57]
            dfm[7]=line2[57:64]
            dfm[8]=line2[64:71]
            dfm[9]=line2[71:78]
            dfm[10]=line2[78:85]
            dfm[11]=line2[85:92]



            dfn[1] = dfm[imo];

            # multiprint(dfm=dfm)

            try:
                ix = pcn.find("A")
                if (ix > -1):
                    pcn = pcn[ix+1:]


                d1 = float(pcn)
                d2 = float(dfn[1])
                d2 = d2 * 0.01



                d3 = d1 - d2
                dfn[1] = round_it(d3, 2)

                # multiprint(pcn=pcn, d1=d1 ,d2=d2 ,d3=d3 , dfm=dfm, dfn_1=dfn[1])

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                dfn[1] = " "
                
        else: 
            dfn[1] = " "

        # multiprint(dfn_1=dfn[1] ) 

    except Exception as err: #TODO: Consider traceback.format_exc()
            print("error: {}".format(traceback.format_exc()))

    return dfn


############################################################



def multiprint(**kwargs):
    print('-'*30)
    for k, v in kwargs.items():
        try:
            print("{}: {}".format(k,v))
        except Exception as err: #TODO: Consider traceback.format_exc()
            print("error printing: {}".format(traceback.format_exc()))


get8110shdd("FMC00914213")

print(computeDivDFN("4801", "    30M", "   89", str(imo)))

loadHddNorm()

# print("HHDVAL{} \nHDDID {}".format(hddval, hddid))

# getMlyNormals8110( "CAW00064757" )
print(getMlyNormals8110( "CAW00064757" ))

# getMlyNormals9121("US1COLR0767")
print(getMlyNormals9121("AQW00061705"))
print(getMlyNormals9121("US1COLR0767"))

print(getTempNorm8110("CAW00064757","    30M", "    1", 1))

print(getTempNorm9120("AQW00061705", "    30M", "    1", 1))

print(getTempNorm7100("023393", "    58M", "    A6", 1))
