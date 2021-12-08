# -*- coding: utf-8 -*-
"""
Created on Thu Dec  2 21:37:44 2021

@author: haoch
"""
import requests
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon,Point
import json
import re
#%%
deal=pd.read_excel('D:/RA Python/M&A filter/result/munual check/2021_Nov_25_spouse_panel/result/deal_panel.xlsx')
#%%
def get_add_map(deal):
    add_list=['HQ before MA','commuter_HM','year-2','year-1', 'MA year', 'year+1', 'year+2', 'year+3', 'year+4', 'year+5', 'Spouse year-2','Spouse year-1', 'Spouse MA year', 'Spouse year+1', 'Spouse year+2', 'Spouse year+3', 'Spouse year+4', 'Spouse year+5']
    d=[deal[i] for i in add_list]
    d=pd.concat(d).drop_duplicates().dropna()
    d=pd.DataFrame({'add':d})
    return d
d=get_add_map(deal)

#%%
def getcoor(address):
    key=' '
    address=address.replace('#','NO.')
    url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address,key)
    req = requests.get(url)
    res = req.json()
    if res['status']=='OK':
        result = res['results'][0]
        coor=str([result['geometry']['location']['lat'],result['geometry']['location']['lng']])
        google_add=result['formatted_address']
        return coor,google_add
    else:
        print(url,address)
        return np.nan,np.nan
    
def build_df (test,name):
    if 'geometry' in test.columns:
        test.drop('geometry', axis=1, inplace=True)
    geometry=test[name].apply(lambda x: Point(json.loads(x)[::-1]))
    test = gpd.GeoDataFrame(test, crs="EPSG:4326", geometry=geometry).to_crs(3310)
    return test
#%%
def add_map(d):
    for ind, row in d.iterrows():
        add=d.loc[ind,'add']
        add=re.sub(' +', ' ',add)
        coor,google_add=getcoor(add)
        d.loc[ind,'coor']=coor
        d.loc[ind,'Google Address']=google_add

#%%
add_map=build_df(d,'coor')
#%%
def get_coor(add,add_map):
    sub=add_map[add_map['add']==add]
    if len(sub)>0:
        coor=sub['geometry'].iloc[0]
    else:
        coor=np.nan
    return coor

def get_distance(add1,add2,add_map):
    if pd.isnull(add1) or pd.isnull(add2):
        return np.nan
    cor1=get_coor(add1,add_map)
    cor2=get_coor(add2,add_map)
    distance=cor1.distance(cor2)/1609.34
    return distance
#%%
for ind, row in deal.iterrows():
    MA_year=deal.loc[ind,'MA year']
    if pd.isnull(MA_year):
        MA_year=deal.loc[ind,'commuter_HM']
    HQ_add=deal.loc[ind,'HQ before MA']
    deal.loc[ind,'HM_HQ_distance']=get_distance(MA_year,HQ_add,add_map)
    deal.loc[ind,'CEO_Spouse_distance_year-2']=get_distance(deal.loc[ind,'year-2'],deal.loc[ind,'Spouse year-2'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year-1']=get_distance(deal.loc[ind,'year-1'],deal.loc[ind,'Spouse year-1'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_MA_year']=get_distance(deal.loc[ind,'MA year'],deal.loc[ind,'Spouse MA year'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year+1']=get_distance(deal.loc[ind,'year+1'],deal.loc[ind,'Spouse year+1'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year+2']=get_distance(deal.loc[ind,'year+2'],deal.loc[ind,'Spouse year+2'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year+3']=get_distance(deal.loc[ind,'year+3'],deal.loc[ind,'Spouse year+3'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year+4']=get_distance(deal.loc[ind,'year+4'],deal.loc[ind,'Spouse year+4'],add_map)
    deal.loc[ind,'CEO_Spouse_distance_year+5']=get_distance(deal.loc[ind,'year+5'],deal.loc[ind,'Spouse year+5'],add_map)
    


