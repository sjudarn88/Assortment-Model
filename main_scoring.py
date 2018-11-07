import pandas as pd
import os

df=pd.read_gbq("SELECT locn_nbr FROM apparel_ao_v2_allstores.all_stores", project_id="syw-analytics-ff",dialect="standard")
stores=df["locn_nbr"]
'''
1	102	APPAREL - CHILDRENS / KIDS	29	NWBN,TDDL,JUV,CHAR/STPLS	 
2	102	APPAREL - CHILDRENS / KIDS	40	BOY'S APPAREL	 
3	102	APPAREL - CHILDRENS / KIDS	77	GIRL'S APPAREL	 
4	102	APPAREL - CHILDRENS / KIDS	49	CHILDREN'S FURNITURE


1	103	APPAREL - INTIMATE APPAREL	38	SLEEPWEAR/ROBES	 
2	103	APPAREL - INTIMATE APPAREL	18	BRAS/PANTIES/DAYWEAR
'''
nystores = [1004,1114,1333,1364,1404,1414,1504,1584,1624,1674,1733,1894,1924,1944,1984,2173,2453,2584,2593,2683,2744,2933]

#stores = [2744, 2933]
#soar = 102
#divs = [40, 77, 29, 49]

stores = list(set(stores) - set(nystores))

soar = 103
divs = [38] #18

#soar = 104
#divs = ["41_43", "33", "45"]

#backward store list
stores = list(reversed(stores))
#print(df["locnnbr"])

for store in stores:
    print ("=================================================")
    print(store)
    if store == 1221:
        break
    #if store in [1905, 1264, 1114, 1733, 1008, 1478, 1125, 1358]:
    #    continue
    ###"a" is to append a file
    with open("progress.txt", "a") as myfile:
        myfile.write("STORE :" +str(store) + "\n")
    for div in divs:
        os.system("/home/peddakota_vikash/anaconda3/bin/python main.py " + str(store) + " " + str(div) + " " + str(soar))
    #break 
