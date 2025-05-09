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


# Method
# get8110shdd - get ??? for 1981 - 2010
#  @param gid - GHCN Daily ID
#  @return string

def get8110shdd(gid: str):
    dat = ""

    try:
        # Actual Path to use
        fn = "/" + os.path.join("data", "ops", "norms", "1981-2010", "products", "station", gid + ".normals.txt")

        # script_dir = os.path.dirname(os.path.abspath(__file__))
        # fn = os.path.join(script_dir, gid + ".normals.txt")
        
        print(fn)

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




# Method
# computeDivDFN - Compute Divisioanl DFN.
#   		@param id - 4 character string
#   		@param atmp
#   		@param pcn
#   		@param mo
#   		@return dfn - Array of strings

# static String[] computeDivDFN(String id, String atmp, String pcn, String mo)
def computeDivDFN( id: str,  atmp: str, pcn: str,  mo: str):
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
                
                    # print("tid {}, tt1 {}, tt2 {}".format(tid, tt1, tt2))

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


        print("line2 {}".format(line2))
        print("line3 {}".format(line3))

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

        
            print("dfm: {}", dfm)

            dfn[0] = dfm[imo-1];

            print("dfn: {}".format(dfn))

            try:
                ix = atmp.index("M")
                print(ix)
                if (ix > -1):
                    atmp = atmp[0:ix]
                
                print(atmp)

                d1 = float(atmp)
                d2 = float(dfn[0])
                d2 = d2 * 0.1

                print ("{} {}".format (d1, d2))

                d3 = d1 - d2
                dfn[0] = round_it(d3, 1)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(err))
                dfn[0] = " "
              


            print("{} {}".format(dfn[0], type(dfn[0])))
        else: 
            dfn[0] = " "


        
        print("line3")
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

        
            print("dfm: {}".format( dfm))

            dfn[1] = dfm[imo-1];

            print("dfn: {}".format(dfn))

            try:
                # ix = atmp.index("M")
                # print(ix)
                # if (ix > -1):
                #     atmp = atmp[:ix]
                
                print(atmp)

                d1 = float(pcn)
                d2 = float(dfn[1])
                d2 = d2 * 0.01

                print ("{} {}".format (d1, d2))

                d3 = d1 - d2
                dfn[1] = round_it(d3, 2)

            except Exception as err:  #Left as generic exception for now instead of Java's NumberFormatException
                print("error: {}".format(traceback.format_exc()))
                dfn[1] = " "
              


            print("{} {}".format(dfn[1], type(dfn[1])))
        else: 
            dfn[1] = " "

    except Exception as err: #TODO: Consider traceback.format_exc()
        print(err)
    
    return dfn


def loadHddNorm():

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

    # print("{}  {}".format(hddval, hddid))



# NOTE: This should be in ghcnDataBrowser.java
# Method
# getMlyNormals8110 - Get monthly 81-2010 normals.
#       @param gid  - GHCND ID.
#       @return List of strings
def getMlyNormals8110(gid: str):
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

        # print("{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n".format(tmax, tmin, tavg, cldd, hldd, prcp, snow, ok))      

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
# Method
# getMlyNormals8110 - Get monthly 81-2010 normals.
# @param gid  - GHCND ID.
		#   @return

def getMlyNormals9121(gid: str):
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

        # print("{}\n{}\n{}\n{}\n{}\n{}\n{}\n".format(tmax, tmin, tavg, cldd, hldd, prcp, snow))      

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
                        rec = val.strip() # Note that this leaves leading WS. This is how the OG code is?
                        # '  5580,5096,5673,5385,5332,4965,4929,4929,4980,5285,5250,5549'
                        # Executive decision made, hldd has it. probably a mistake to omit it.
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

    


# //****************************************************************************	   
# /**method
#    getTempNorm8110 - Get temp 71 -2000 Norms.
#      @param id - ghcnd id
#      @param atmp -  avg Temp
#      @param pcn - Precip total
#      @param iMo - Month, integer.
# */
def getTempNorm8110(id: str, atmp: str, pcn: str, imo: int):
    dfn = []

    mdfn = []
############################################################


# get8110shdd("FMC00914213")

# print(computeDivDFN("4801", "    30M", "   89", str(imo)))

# loadHddNorm()

# print("HHDVAL{} \nHDDID {}".format(hddval, hddid))

# getMlyNormals8110( "CAW00064757" )
print(getMlyNormals8110( "CAW00064757" ))

# getMlyNormals9121("US1COLR0767")
print(getMlyNormals9121("AQW00061705"))
print(getMlyNormals9121("US1COLR0767"))
