import pandas as pd 
import scipy.spatial
import json
import numpy as np 
from itertools import combinations 
from collections import Counter
import os

project_dir = '/shared/2/projects/cross-lingual-exchange/'
data_dir = os.path.join(project_dir,'data','country_language_metadata')
effects_dir = os.path.join(project_dir,'results')
data_file = os.path.join(effects_dir,'effects_with_country_vars_11-08-21.tsv')
df = pd.read_csv(data_file,sep='\t')
df = df.drop(columns=['Unnamed: 0'])
print(df)

effect_files = {}
effect_files['betweenness'] = 'att_betweenness_11-02-2021.tsv'
effect_files['domain'] = 'att_domains_top100_with_rt_11-02-2021.tsv'
effect_files['hashtag'] = 'att_temporal_hashtags_top100_with_rt_11-02-2021.tsv'
effect_files['entertainment'] = 'entertainment_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['politics'] = 'politics_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['sports'] = 'sports_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['promotion'] = 'promotion_att_temporal_hashtag_top100_with_rt.tsv'

for outcome in effect_files.keys():
    att_file = effect_files[outcome]
    counts = pd.read_csv(os.path.join(effects_dir,att_file),sep='\t')
    if outcome != 'betweenness':
        counts['Treated'] = counts['Treated_Outcome0'] + counts['Treated_Outcome1']
        counts = counts[(counts['Treated_Outcome1']!=0) & (counts['Treated_Outcome1']!=0)]
    df1 = df.merge(counts,on=['Country Pair','Country'])
    out_file = os.path.join(effects_dir,f'{outcome}_effects_with_country_vars_11-08-21_small.tsv')
    df1.to_csv(out_file,sep='\t')

# for outcome in effect_files.keys():
#     att_file = effect_files[outcome]
#     counts = pd.read_csv(os.path.join(effects_dir,att_file),sep='\t')
#     if outcome != 'betweenness':
#         counts['Treated'] = counts['Treated_Outcome0'] + counts['Treated_Outcome1']
#         counts = counts[(counts['Treated_Outcome1']!=0) & (counts['Treated_Outcome1']!=0)]
#     df2 = df.merge(counts,on=['Country Pair','Country'])
#     df3 = df2.loc[df2.index.repeat(df2['Treated'])].assign(fifo_qty=1).reset_index(drop=True)
#     out_file = os.path.join(effects_dir,f'{outcome}_effects_with_country_vars_11-08-21.tsv')
#     df3.to_csv(out_file,sep='\t')
#     print(df3)
#     print(set(df3['Country Pair']))
