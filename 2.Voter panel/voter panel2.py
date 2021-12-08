# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 15:40:38 2021

@author: haoch
"""
#%%
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import re
from cleanco import prepare_terms, basename
#%%
panel=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/target/panel.xlsx')
#%%
deal=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/result/deal_panel.xlsx')
#%%
HQ=pd.read_excel('D:/RA Python/CEO/HQ_add2.xlsx')
#%%
deal=deal[~deal['LexID'].isnull()]
#%%
commuter['LexID']=commuter['LexID'].apply(lambda x:get_int(x))
#%%
def get_short_panel(deal,panel,commuter,HQ):
    terms = prepare_terms()
    for ind, row in deal.iterrows():
        LexID=deal.loc[ind,'LexID']
        year=deal.loc[ind,'Date Announced'].year
        com_nm=deal.loc[ind,'Target Full Name match']
        com_nm=basename(str(com_nm), terms, prefix=False, middle=False, suffix=True)
        # get panel info
        minus2,minus1,current,plus1,plus2,plus3,plus4,plus5=get_add(panel,LexID,year,'CEO add')
        deal.loc[ind,'year-2']=minus2
        deal.loc[ind,'year-1']=minus1
        deal.loc[ind,'MA year']=current
        deal.loc[ind,'year+1']=plus1
        deal.loc[ind,'year+2']=plus2
        deal.loc[ind,'year+3']=plus3
        deal.loc[ind,'year+4']=plus4
        deal.loc[ind,'year+5']=plus5
        # spouse panel info 
        minus2,minus1,current,plus1,plus2,plus3,plus4,plus5=get_add(panel,LexID,year,'Spouse add')
        deal.loc[ind,'Spouse year-2']=minus2
        deal.loc[ind,'Spouse year-1']=minus1
        deal.loc[ind,'Spouse MA year']=current
        deal.loc[ind,'Spouse year+1']=plus1
        deal.loc[ind,'Spouse year+2']=plus2
        deal.loc[ind,'Spouse year+3']=plus3
        deal.loc[ind,'Spouse year+4']=plus4
        deal.loc[ind,'Spouse year+5']=plus5
        # get commuter info 
        # HQ_add,HM_add,Company,CIK=get_commuter(LexID,commuter,com_nm)
        # deal.loc[ind,'commuter_HQ']=HQ_add
        # deal.loc[ind,'commuter_HM']=HM_add
        # deal.loc[ind,'commuter_Com']=Company
        # deal.loc[ind,'commuter_CIK']=CIK
        # if not pd.isnull(CIK):
        #     HQ_add=get_HQ_add(CIK,year,HQ)
        #     deal.loc[ind,'HQ before MA']=HQ_add
#%%
def get_HQ_add(CIK,year,HQ):
    sub=HQ[(HQ['CIK']==CIK)&(HQ['report_yr']==(year-1))]
    if len(sub)>0:
        HQ_add=sub['correct_add'].iloc[0]
        return HQ_add
    else:
        return np.nan
#%%
def get_add(panel,LexID,year,add_col):
    sub2=panel[panel['LexID']==LexID]
    try:
        minus2=sub2[sub2['Year']==(year-2)][add_col].iloc[0]
    except:
        minus2=np.nan
    try:
        minus1=sub2[sub2['Year']==(year-1)][add_col].iloc[0]
    except:
        minus1=np.nan
    try:
        current=sub2[sub2['Year']==(year)][add_col].iloc[0]
    except:
        current=np.nan
    try:
        plus1=sub2[sub2['Year']==(year+1)][add_col].iloc[0]
    except:
        plus1=np.nan
    try:
        plus2=sub2[sub2['Year']==(year+2)][add_col].iloc[0]
    except:
        plus2=np.nan
    try:
        plus3=sub2[sub2['Year']==(year+3)][add_col].iloc[0]
    except:
        plus3=np.nan
    try:
        plus4=sub2[sub2['Year']==(year+4)][add_col].iloc[0]
    except:
        plus4=np.nan
    try:
        plus5=sub2[sub2['Year']==(year+5)][add_col].iloc[0]
    except:
        plus5=np.nan
    return minus2,minus1,current,plus1,plus2,plus3,plus4,plus5
    
#%%
def get_commuter(LexID,commuter,com_nm):
    terms = prepare_terms()
    sub=commuter[commuter['LexID']==LexID]
    if len(sub)==1:
        HQ_add=sub['HQ address'].iloc[0]
        HM_add=sub['Home address'].iloc[0]
        CIK=sub['CIK'].iloc[0]
        Company=np.nan
    else:
        mask=sub['Company'].apply(lambda x:if_same(\
                        basename(str(x), terms, prefix=False, middle=False, suffix=True)
                                                   ,com_nm,num=90))
        sub1=sub[mask]
        if len(sub1)==0:
            return np.nan,np.nan,np.nan,np.nan
        HQ_add=sub1['HQ address'].iloc[0]
        HM_add=sub1['Home address'].iloc[0]
        Company=sub1['Company'].iloc[0]
        CIK=get_int(sub1['CIK'].iloc[0])
    return HQ_add,HM_add,Company,CIK
#%%
def get_int(x):
    try:
        return int(x)
    except:
        return x
def if_same(x,y,num=90):
    if fuzz.ratio(str(x).upper(),str(y).upper())<num:
        return False
    else:
        return True
#%%
def fill_all(panel):
    panel['CEO add']=panel['voter address']
    panel['Spouse add']=panel['spouse voter address']
# .apply(lambda x :re.sub('\d{5}(?:[-\s]\d{4})?$','',x) if type(x)==str else x)
    group=panel.groupby('LexID')
    for lex,df in group:
        write_panel('CEO add',panel,df)
        write_panel('Spouse add',panel,df)
    panel['CEO add']=panel.apply(lambda x:\
            x['Household Max Address'] if pd.isnull(x['CEO add']) else x['CEO add'],axis=1)
#%%
def write_panel(col,panel,df1):
    df=df1[~df1[col].isnull()]
    for i in range(len(df)):
        CEO_add=df[col].iloc[i]
        start_i=df.index[i]
        sub=df.iloc[(i+1):]
        mask=sub[col].apply(lambda x:if_same(x,CEO_add))
        end_i=sub[mask].index.max()
        if (not pd.isnull(end_i) )and start_i<end_i:
            ind_list=list(range(start_i,end_i+1))
            for ind in ind_list:
                if pd.isnull(panel.loc[ind,col]):
                    panel.loc[ind,col]=CEO_add
#%%
def clean_add(df,col):
    for i in range(1,len(df)):
        current_add=df[col].iloc[i]
        last_add=df[col].iloc[i-1]
        ind=df.index[i]
        x=re.sub('\d{5}(?:[-\s]\d{4})?$','',str(current_add))
        y=re.sub('\d{5}(?:[-\s]\d{4})?$','',str(last_add))
        if if_same(x,y):
            df.loc[ind,col]=last_add
#%%
panel.drop_duplicates(subset=['LexID','year'])
#%%
panel.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/target/panel.csv',index=None)
#%%
deal.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/result/deal_panel.csv',sep='|',index=None)