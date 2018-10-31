import sys
import pandas as pd
import numpy as np
from sklearn import linear_model

store = sys.argv[1]
div = sys.argv[2]
soar = sys.argv[3]
season = sys.argv[4]

result = pd.DataFrame()
resultfile2 = './' + store + '_' + soar + "_" + div + "_" + season + '/elasticities_' + store + '_' +soar + "_" + div +"_"+ season + '_pair.csv'
result2 = pd.DataFrame()

elasticfile = './' + store + '_' +  soar + "_" + div + "_" + season + '/elasticity_data_store_' + store + '_' + soar + '_' + div + '_wide.csv'

print("Reading file")
analysisFile = pd.DataFrame()
analysisFile = pd.read_csv(elasticfile, index_col=None, header=0)
