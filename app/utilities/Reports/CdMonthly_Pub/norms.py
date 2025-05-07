import os
import traceback

# Temporary File for Norms functions.

# NOTE: replace these with external functions or variables. 
CoopToGhcn_defaultPath = "/" + os.path.join("data", "ops", "ghcndqi")
imo = 2

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
                ix = atmp.index("M")
                print(ix)
                if (ix > -1):
                    atmp = atmp[:ix]
                
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



############################################################


# get8110shdd("FMC00914213")

print(computeDivDFN("4801", "    30M", "   89", str(imo)))