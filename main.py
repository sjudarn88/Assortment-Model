import os
import datetime
import sys

store = str(sys.argv[1]) #str(1125)   # 4 digit store name
div = str(sys.argv[2]) #'41_43'    # Seperate by _ if more than 1 division
soar = str(sys.argv[3]) #str(104)   # Soar number of the division 
desc= 'Basic_items_Apr_Mar_data'

season = 'fw'

v='1'

with open("progress.txt", "a") as myfile:
    myfile.write("started " + store+" "+ soar +" " + div + " " + season + " " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")

print("Create Target Directory")
os.system("mkdir " + store+"_"+ soar + "_" + div+"_"+season)

print("Create Data for Elasticities")
if store not in ['9999']:
    os.system("sh ./apparel_AO_v2_0430_Scripts_s1_a_prep_elasticity_data.sh " + store + " " + div + " " + soar + " " + season)
   
print("Prepare Elasticity Data in Wide format and save to local")
if store not in ['1024999']:
    os.system("sh ./apparel_AO_v2_0430_Scripts_s1_b_elasticity_data_wide.sh apparel_AO_v2_0430_Scripts_Elasticity_wide.R " + store + " " + div + " " + soar + " " + season)
    
print("Compute Elasticities")
if store not in ['10249']:
    os.system("sh ./apparel_AO_v2_0430_Scripts_s1_c_compute_elasticities.sh " + store + " " + div + " " + soar + " " + season)
    
print("Create Store Level Data")
#if store not in ['1003', '1004']:
os.system("sh ./apparel_AO_v2_0430_Scripts_s2_prep_store_data.sh " + store + " " + div + " " + soar + " " + season)

print("Run Optimum Assortment")
os.system("sh ./apparel_AO_v2_0430_Scripts_s3_ao.sh " + store + " "  + div + " " + soar + " "+ season)

with open("progress.txt", "a") as myfile:
    myfile.write("Finished " + store +" " + div + " " + soar + " " + season + " " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n \n \n" )
    
