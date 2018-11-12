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

elasticfile = './' + store + '_' +  soar + "_" + div + "_" + season + '/_' + store + '_' + soar + '_' + div + '_wide.csv'

print("Reading file")
analysisFile = pd.DataFrame()
analysisFile = pd.read_csv(elasticfile, index_col=None, header=0)

#columns with cluster numbers
cols = [col for col in analysisFile.columns if col not in ['Unnamed: 0', 'wk_nbr', 'div_ln_cls', 'log_UNITS']]

#for store __,delete 14 cols out of 36 cols.
#each wk_nbr has duplicate values. We are only considering clusters with more than 30 wk_nbrs.
del_cols = dict()
for col in cols:
    if analysisFile.drop_duplicates(["wk_nbr", col]).loc[:,['wk_nbr', col]][col].count() < 30:
        del_cols[col] = analysisFile.drop_duplicates(["wk_nbr", col]).loc[:,['wk_nbr', col]][col].count()


print("Remove products")
analysisFile = analysisFile[~analysisFile.div_ln_cls.isin(del_cols.keys())]
analysisFile.drop(del_cols.keys(), axis = 1, inplace = True)

#imputing values for NAs
print("imputing_values")
imputing_values = dict()
for col in cols:
    if col in del_cols.keys():
        continue
    imputing_values[col] = analysisFile.drop_duplicates(["wk_nbr", col]).loc[:,['wk_nbr', col]][col].mean()
    if np.isnan(imputing_values[col]):
        imputing_values[col] = 0

analysisFile.fillna(imputing_values, inplace = True)

len(analysisFile.div_ln_cls.unique())

previous_cols = cols.copy()


cols = [x for x in cols if x not in del_cols.keys()]

print("calculate elasticities")

t = 0
div_count = 0
for name, group in analysisFile.groupby('div_ln_cls', as_index=False):
    print("==========================")
    print(div_count,"...........",name)
    print(group.shape)
    coef = []
    partData = pd.DataFrame()
    partData = group[group['wk_nbr'] > 201600]
    #print(partData.shape)

    classData = pd.DataFrame()
    classData = partData[cols].copy()#.replace('NA', 0)
    #print(classData.shape)
    classData['week'] = partData['wk_nbr']
    classData['unit'] = partData['log_UNITS']
    #print(classData.shape)

    sortData = pd.DataFrame()
    sortData = classData.sort_values('week')
#    print(sortData.shape[0])
    weeklist = sortData.week.unique()
#    print(weeklist)

#Generating 23000 of 0s list.
    k = 0
    coef = [0]*23000
    startwk = [0]*23000
    endwk = [0]*23000

    for i in np.arange(0, sortData.shape[0]-2, 1):
#        print(weeklist[i])

        for j in np.arange(i+3, sortData.shape[0]-1, 1):
#            print(weeklist[j])
#            print(k)
            
            startwk[k] = weeklist[i]
            endwk[k] = weeklist[j]
#            print(startwk[k])
#            print(endwk[k])

            dataset = pd.DataFrame()
            dataset = sortData[(sortData['week']>=startwk[k]) & (sortData['week']<=endwk[k])]
            #print(dataset.shape)
            #print(len(dataset.columns))
            #print(len(cols))
            clf = linear_model.LinearRegression()
            #print(clf)
            clf.fit(dataset[cols], dataset['unit'])
            loc = dataset[cols].columns.get_loc(name)
            coef[k] = clf.coef_[loc]
#            print(coef[k])

            k = k+1

    min_coef = min(coef)
    #print("self coef.......", coef)
    print("min self.....", min_coef)
    index = coef.index(min_coef)
    #print(index)
        
    mindataset = pd.DataFrame()
    mindataset = sortData[(sortData['week']>=startwk[index]) & (sortData['week']<=endwk[index])]
#    print(mindataset.shape)

    if mindataset.shape[0] > 0:
        print(mindataset.shape)
        minclf = linear_model.LinearRegression()
        minclf.fit(mindataset[cols], mindataset['unit'])
        coeflist = minclf.coef_
        #print(coeflist)
    
        result.loc[t, 'div_ln_cls'] = name
        result.loc[t, 'own_elasticity'] = min_coef
        result.loc[t, 'week_start'] = startwk[index]
        result.loc[t, 'week_end'] = endwk[index]
        s = 0
        for colsname in cols:
            result.loc[t, colsname] = coeflist[s]
            
            result2.loc[t*len(cols)+s, 'div_ln_cls_1'] = name
            result2.loc[t*len(cols)+s, 'div_ln_cls_2'] = colsname
            result2.loc[t*len(cols)+s, 'elasticity'] = coeflist[s]
            
            s = s+1
    else:
        result.loc[t, 'div_ln_cls'] = name
        result.loc[t, 'own_elasticity'] = 'null'
        result.loc[t, 'week_start'] = 'null'
        result.loc[t, 'week_end'] = 'null'
        s = 0
        for colsname in cols:
            result.loc[t, colsname] = 'null'
            
            result2.loc[t*len(cols)+s, 'div_ln_cls_1'] = name
            result2.loc[t*len(cols)+s, 'div_ln_cls_2'] = colsname
            result2.loc[t*len(cols)+s, 'elasticity'] = 'null'
            
            s = s+1

    t = t+1
    div_count = div_count + 1
#    if t == 1:
#        break




zero_pairs = []
for colsname1 in del_cols:
    for colsname2 in cols+list(del_cols.keys()):
        pair = dict()
        pair["div_ln_cls_1"] = colsname1
        pair["div_ln_cls_2"] = colsname2
        pair["elasticity"] = 0
        zero_pairs.append(pair)  

zero_pairs_2 = []
for colsname1 in cols:
    for colsname2 in del_cols:
        pair = dict()
        pair["div_ln_cls_1"] = colsname1
        pair["div_ln_cls_2"] = colsname2
        pair["elasticity"] = 0
        zero_pairs.append(pair)

zero_df = pd.DataFrame(zero_pairs)
zero_df2 = pd.DataFrame(zero_pairs_2)
final = pd.concat([result2, zero_df, zero_df2], axis = 0)
print(result2.shape)
print(zero_df.shape)
print(final.shape)
print(len(cols))

final.to_csv(resultfile2, mode='w', index=False)
