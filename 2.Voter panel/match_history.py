# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 11:50:45 2021

@author: haoch
"""
#%%
import pandas as pd
import numpy as np
from dateutil import parser
import json
#%%
panel=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/data/lex panel.xlsx')
# CEO voter data
# voter=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/data/voter.xlsx')
# CEO spouse voter data
voter=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/target/spouse voter.xlsx')
#%%
panel['spouse voter address']=np.nan
#%%
d=pd.read_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/Nevada1.csv'\
              )
#%%
# NV=pd.read_csv('D:/RA Python/CEO/Voter history/voter history/Nevada/history/voter_history_9_2020.csv')
#%%
def parse_year(date):
    try:
        year=parser.parse(str(date)).year
        return year
    except:
        return 0
def get_int(x):
    try:
        return int(x)
    except:
        return x
#%%
def get_panel(voter=voter,panel=panel,\
                  state='IL',ID='SUID',electDate='ElectionDate',person='voter address'):
    state_map=json.load(open('D:/RA Python/Distance Test1/code/states_hash.json'))
    # vote_his=pd.read_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/{state}.csv'\
    #                      .format(state=state_map.get(state)))
    vote_his=pd.read_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/{state}.csv'\
                         .format(state=state_map.get(state)))
    voter['RegistrantID']=voter['RegistrantID'].apply(lambda x: str(get_int(x)))
    sub=voter[voter['State']==state]
    vote_his[ID]=vote_his[ID].apply(lambda x: str(get_int(x)))
    panel_g=panel.groupby('LexID')
    for ind, row in sub.iterrows():
        LexID=sub.loc[ind,'LexID']
        Reg_ID=sub.loc[ind,'RegistrantID']
        start=parser.parse(str(sub.loc[ind,'RegistrationDate'])).year
        Add=sub.loc[ind,'street_address'].strip()+','+sub.loc[ind,'City'].strip()+','+sub.loc[ind,'State'].strip()
        end=parse_year(sub.loc[ind,'LastVoteDate'])
        
        sub_p=panel_g.get_group(LexID)
        history=vote_his[vote_his[ID]==Reg_ID]
        # get a list of voting year
        #general method
        # history['year']=history[electDate].apply(lambda x: parse_year(x))
        # his_year=history[history['year']>=start]['year'].to_list()
        #NY voter
        # vote_record=history["44"].iloc[0]
        # his_year=get_num(vote_record)

        ##CT / KS /MO voter
        his_year=get_year_list(history)
        for i, r in sub_p.iterrows():
            if (sub_p.loc[i,'Year']==start or sub_p.loc[i,'Year']==end) and\
                len(str(panel.loc[i,person]))<4:
                print('res',Add)
                panel.loc[i,person]=Add
            if sub_p.loc[i,'Year'] in his_year and len(str(panel.loc[i,person]))<4:
                print(Add)
                panel.loc[i,person]=Add
#%%
def get_vote_add(voter,panel,person):
    panel_g=panel.groupby('LexID')
    for ind, row in voter.iterrows():
        LexID=voter.loc[ind,'LexID']
        start=parser.parse(str(voter.loc[ind,'RegistrationDate'])).year
        Add=voter.loc[ind,'street_address'].strip()+','+voter.loc[ind,'City'].strip()+','+voter.loc[ind,'State'].strip()
        end=parse_year(voter.loc[ind,'LastVoteDate'])
        sub_p=panel_g.get_group(LexID)
        for i, r in sub_p.iterrows():
            # if (sub_p.loc[i,'Year']==start or sub_p.loc[i,'Year']==end) and len(str(panel.loc[i,'spouse voter address']))<4:
            if (sub_p.loc[i,'Year']==start or sub_p.loc[i,'Year']==end) and len(str(panel.loc[i,'voter address']))<4:
                print('res',Add)
                # panel.loc[i,'spouse voter address']=Add
                panel.loc[i,person]=Add
#%%
# used for NY voting history data
def get_num(hist):
    vote_list=str(hist).split(';')
    year_list=[]
    for vote in vote_list:
        unit=[int(s) for s in vote.split() if s.isdigit()]
        if len(unit)>0:
            year_list=year_list+unit
    return year_list
#%%
# for Connecticut vote history
# get all date data from a row
def get_year_list(df):
    year_list=[]
    df=df.reset_index(drop=True)
    for i in range(len(df.columns)):
        row=df.iloc[0,i]
        year=parse_year(row)
        #print(row)
        if year==2021:
            year=2000
        if year>1989:
            year_list.append(year)
    return year_list
#%%
get_panel(state='MO',ID='Voter ID',electDate='election_lbl',person='spouse voter address')
#%%
panel['voter address']=panel['voter address'].apply(lambda x: x.replace('.0' , '') if type(x)==str else x)
panel['spouse voter address']=panel['spouse voter address'].apply(lambda x: x.replace('.0' , '') if type(x)==str else x)
#%%
panel.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/target/panel.csv',sep='|')