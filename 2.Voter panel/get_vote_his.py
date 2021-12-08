# -*- coding: utf-8 -*-
"""
Created on Sat Nov 20 12:28:49 2021

@author: haoch
"""
import pandas as pd
import numpy as np
import json
import os 
import glob
import pandas._libs.lib as lib
import warnings
import multiprocessing as mp
warnings.filterwarnings("ignore")

#%%
def get_target_panel(target,panel):
    target=target[~target['LexID'].isna()]
    target['LexID']=target['LexID'].apply(lambda x: int(x))
    target['year']=target['Date Announced'].apply(lambda x: x.year)
    target_p=panel[panel['LexID'].isin(target['LexID'].tolist())]
    group=target_p.groupby('LexID')
    target_p['MA flag']=0
    for ind,row in target.iterrows():
        LexID=target.loc[ind,'LexID']
        year=target.loc[ind,'year']
        try:
            df=group.get_group(LexID)
        except:
            continue
        for i,r in df.iterrows():
            if df.loc[i,'Year']==year:
                target_p.loc[i,'MA flag']=1
    return target_p

#%%
def get_vote_his(df):
    try:
        state_map=json.load(open('D:/RA Python/Distance Test1/code/states_hash.json'))
        state=df['State'].iloc[0]
        full_state=state_map.get(state)
        t_list=df['RegistrantID'].to_list()
        p=glob.glob("D:/RA Python/CEO/Voter history/voter history/{st}/*/history".format(st=full_state))
        if len(p)==0:
            p=glob.glob("D:/RA Python/CEO/Voter history/voter history/{st}/history".format(st=full_state))
            if len(p)==0:
                print('no file',full_state)
        for path in p:
            write_p='D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/{st}.csv'.format(st=full_state)
            for filename in os.listdir(path):
                file_p=os.path.join(path, filename)
                print(file_p)
                df=get_file(path,filename)
                print(len(df))
                voterID=['VoterID','cde_gender','Voter ID Number ','StateVoterID','SOS_VOTERID','Voter ID Number','VUID','RegistrantID','VOTER_ID','voter_id','Res ID','UNIQUE-ID','Voter ID','REGISTRATION_NUMBER','SUID','RegistrantID','text_registrant_id','VTRID','VOTER_IDENTIFICATION_NUMBER','VoterId','VoterID','voter_id','voter_reg_num','VOTER_ID','ID Number','VOTER ID','SOS_VoterID']
                cols = [col for col in df.columns if col in voterID]
                if len(cols)==0:
                    print('no ID',full_state,filename)
                    continue
                df=df[~df[cols[0]].isnull()]
                df[cols[0]]=df[cols[0]].apply(lambda x:str(int(x)) if type(x)!=str  else x)
                unit=df[df[cols[0]].isin(t_list)]
                if len(unit)==0:
                    print('not found')
                    continue
                else:
                    unit.to_csv(write_p, mode='a')
    except:
        file1 = open("D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/err.txt","a")
        file1.write(df.head(1)+"\n")
#%%
def get_table(path):
    df=read_table(path,nrows=2)
    if len(df.columns)<2:
        df2=read_table(path,nrows=2,sep='|')
        if len(df2.columns)<2:
            df3=read_table(path,nrows=2,sep=',')
            if len(df3.columns)<2:
                print('wrong sep',path)
                return
            else:
                df=read_table(path,sep=',')
        else:
            df=read_table(path,sep='|')
    else:
        df=read_table(path)
    return df

def read_table(path,sep=lib.no_default,nrows=None,header="infer"):
    try:
        df=pd.read_table(path,nrows=nrows,sep=sep, error_bad_lines=False,header="infer")
    except:
        try:
            df=pd.read_table(path,nrows=nrows,sep=sep,encoding='cp1252', error_bad_lines=False,header="infer")
        except:
            df=pd.read_table(path,nrows=nrows,sep=sep,encoding='cp437', error_bad_lines=False,header=None)
    return df

def get_file(path,filename):
    file_p=os.path.join(path, filename)
    if filename.endswith('.txt') or filename.endswith('.TXT'):
        df=get_table(file_p)
    elif filename.endswith('.csv'):
        try:
            df=pd.read_csv(file_p,error_bad_lines=False,encoding="cp1252")
        except:
            df=pd.read_csv(file_p,error_bad_lines=False)
    elif filename.endswith('.xlsx'):
        try:
            df=pd.read_excel(file_p)
        except:
            print('wrong excel')
    else:
        print('wrong file type',path)
    if len(df.columns)<2:
        print('wrong sep',file_p)
    return df
#%%
def big_process():
    state_map=json.load(open('D:/RA Python/Distance Test1/code/states_hash.json'))
    states={v: k for k, v in state_map.items()}
    names = [states.get(os.path.basename(x).replace('.csv', ''))
             for x in glob.glob('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/*.csv')]  
    voter=pd.read_excel('D:/RA Python/CEO/Voter history/result/Final_match_revised.xlsx')
    target=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/data/lex panel.xlsx')
    target_vote=voter[voter['LexID'].isin(target['LexID'].tolist())]
    target_vote=target_vote[~target_vote['State'].isin(names)]
    grouped=[group for _, group in target_vote.groupby('State')]
    pool = mp.Pool(2)
    pool.map(get_vote_his, grouped)
    pool.close()
    pool.join()

#%%
def spouse_process():
    state_map=json.load(open('D:/RA Python/Distance Test1/code/states_hash.json'))
    states={v: k for k, v in state_map.items()}
    names = [states.get(os.path.basename(x).replace('.csv', ''))
             for x in glob.glob('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/*.csv')]  
    
    voter=pd.read_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/spouse_vote.csv',sep='|')
    voter=voter[~voter['State'].isin(names)]
    grouped=[group for _, group in voter.groupby('State')]
    pool = mp.Pool(1)
    pool.map(get_vote_his, grouped)
    pool.close()
    pool.join()

#%%
if __name__ == '__main__':
    spouse_process()
#%%
#KS voter
path='D:/RA Python/CEO/Voter history/voter history/Missouri/history/PSR_VotersList_10052020_8-58-20 AM.txt'
df=read_table(path=path)
#%%
sub=voter[voter['State']=='MO']
# d=df[df['VoterId'].isin(sub['RegistrantID'].to_list())]
d=df[df['Voter ID'].isin([73146718, 751381763])]
#KS d=df[df['cde_gender']==1249516]
#%%
# munual search nonstandardized data NY
path='D:/RA Python/CEO/Voter history/voter history/New York no ID/history/AllNYSVoter_20200727.txt'
df=pd.read_table(path,error_bad_lines=False,header=None,encoding='cp437',sep=',')
t_list=voter[voter['State']=='NY']['RegistrantID'].to_list()
d=df[df[43].isin(t_list)]
# panel.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_19_CEO add panel/data/target_p.csv')
#%%
# DC voter data
df=pd.read_csv('D:/RA Python/CEO/Voter history/voter history/DC no ID/history/DC_VH_EXPORT(ALL).csv')
t_list=voter[voter['State']=='DC']['RegistrantID'].to_list()
df['VoterID']=df.apply(lambda x:x['REGISTERED']+ str(x['LASTNAME']).strip()+str(x['FIRSTNAME']).strip(),axis=1)
dc=df[df['VoterID'].isin(t_list)]
#%%
d.to_csv('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/Missouri.csv')