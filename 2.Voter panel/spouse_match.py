# -*- coding: utf-8 -*-
"""
Created on Wed Nov 24 21:23:53 2021

@author: haoch
"""
import multiprocessing as mp
import pandas as pd
import numpy as np
import datetime
from dateutil import parser
import json
import glob

#%%
# get spouse city list, zip list and state list
#target_spouse['city']=target_spouse['Street Address'].apply(lambda x: x.split(',')[-2].strip())
# FL=pd.read_csv('D:/RA Python/CEO/Voter history/Combined File\Burlington MA.csv')
#%%
def get_DOY(date):
    try:
        date=parser.parse(date)
        #return np.float64(date.year)
        return str(date.year)
    except:
        return ''
def get_DOM(date):
    try:
        date=parser.parse(date)
        #return np.float64(date.month)
        return str(date.month)
    except:
        return ''  
def get_middle_name(name):
    try:
        name=name.split(',')[1].split()[1]
        return name
    except:
        return np.nan
    
def get_int(x):
    try:
        return int(x)
    except:
        return x

def if_middle(x,y):
    if str(x)=='nan' or pd.isna(x) or str(y)=='nan' or pd.isna(y):
        return 'no middle'
    elif str(x)[0:1].upper()==str(y)[0:1].upper():
        return 1
    else:
        return 0

def get_match_spouse(FL,test):
    state_map=json.load(open('D:/RA Python/Distance Test1/code/states_hash.json',))
    target_state=state_map.get(FL.loc[0,'State'])
    
    test=test[~test['hm state list'].isnull()]
    test=test[test['hm state list'].str.contains(target_state)]  
    
    test=test.rename({'DOB': 'CEO DOB'}, axis=1) 
    test['CEO last name']=test['Full Name'].apply(lambda x: x.split(',')[0])
    test['CEO first name']=test['Full Name'].apply(lambda x: x.split(',')[1].split()[0])
    test['Spouse last name']=test['Spouse full name'].apply(lambda x: x.split(',')[0])
    test['Spouse first name']=test['Spouse full name'].apply(lambda x: x.split(',')[1].split()[0])
    test['CEO DOY']=test['CEO DOB'].apply(lambda x: get_DOY(x))
    test['CEO DOM']=test['CEO DOB'].apply(lambda x: get_DOM(x))
    test['Spouse DOY']=test['Spouse DOB'].apply(lambda x: get_DOY(x))
    test['Spouse DOM']=test['Spouse DOB'].apply(lambda x: get_DOM(x))
    FL['LastName']=FL['LastName'].apply(lambda x:x.upper() if type(x)==str else x)
    FL['FirstName']=FL['FirstName'].apply(lambda x:x.upper() if type(x)==str else x)
    
    demo=pd.merge(test.applymap(str),FL.applymap(str),left_on=['CEO last name','CEO first name'],\
                      right_on=['LastName','FirstName'],how='inner')
    demo_sp=pd.merge(test.applymap(str),FL.applymap(str),left_on=['Spouse last name','Spouse first name']\
                      ,right_on=['LastName','FirstName'],how='inner')
    if len(demo_sp)==0:
        return pd.DataFrame()
    if 'DOB' in FL: 
        demo_sp['voter DOY']=demo_sp['DOB'].apply(lambda x: get_DOY(x))
        demo_sp['voter DOM']=demo_sp['DOB'].apply(lambda x: get_DOM(x))
        demo_sp['DOB_flag']=demo_sp.apply(lambda x: 1 if \
                            (x['voter DOY']==x['Spouse DOY'])\
                            else 0, axis=1)
    elif 'YOB' in FL:
        demo_sp['YOB']=demo_sp['YOB'].apply(lambda x: str(get_int(x)))
        demo_sp['DOB_flag']=demo_sp.apply(lambda x: 1 if x['YOB']==x['Spouse DOY']else 0,axis=1)
    else:
        demo_sp['DOB_flag']='no DOB'

    demo_sp['DOB_flag']=demo_sp.apply(lambda x: 'no DOB' if len(str(x['Spouse DOB']))<3 else x['DOB_flag'], axis=1)
    demo_sp=demo_sp[demo_sp['DOB_flag']!=0]
    if len(demo_sp)==0:
        return pd.DataFrame()
    
    demo_sp['spouse middle name']=demo_sp['Spouse full name'].apply(lambda x: get_middle_name(x))
   
    demo_sp['city_flag']=demo_sp.apply(lambda x: 1 if (str(x['City']).upper() in x['hm city list'].upper()) \
                                 else 0,axis=1)
    demo_sp['zip_flag']=demo_sp.apply(lambda x: 1 if str(x['Zip'])[0:4] in x['zip list'] else 0,axis=1)

    demo_sp['spouse middle name']=demo_sp['Spouse full name'].apply(lambda x: get_middle_name(x))
    demo_sp['middle_flag']=demo_sp.apply(lambda x:if_middle(x['MiddleName'],x['spouse middle name']),axis=1)
    spouse_match=demo_sp.merge(demo[['LexID','street_address']],on=['LexID','street_address'],how='inner')
    spouse_list=spouse_match['RegistrantID'].to_list()
    demo_sp['spouse_flag']=demo_sp['RegistrantID'].apply(lambda x: 1 if x in spouse_list else 0)
    demo_sp=demo_sp[demo_sp['middle_flag']!=0]
    return demo_sp
#%%
def process_data(f):
    test=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/target/target_lex.xlsx', dtype = str)
    open('D:/RA Python/CEO/Voter history/data/log.txt1', "a").write(f+'\n')
    FL = pd.read_csv(f,low_memory=False)
    try:
        result=get_match_spouse(FL,test)
    except:
        open('D:/RA Python/CEO/Voter history/data/log.txt1', "a").write('wrong'+f+'\n')
        result=f
    return result
def multicore():
    pool=mp.Pool(processes=1)
    file_list=glob.glob("D:/RA Python/CEO/Voter history/Combined File/*.csv")
    mult_res = pool.map(process_data,file_list)
    pool.close()
    pool.join()
    return mult_res
#%%
if __name__ == '__main__':
    res_list=multicore()
# target_CEO.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/data/target_lex.csv',sep='|',index=None)