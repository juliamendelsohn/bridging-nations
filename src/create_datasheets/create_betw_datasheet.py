import csv
import os
import pandas as pd 
import ast
from multiprocessing import Pool



"""
Loop through all network subsets
Loop through each country within network subset
Subset dataframes to that country
Combine dataframes
"""

base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
network_stats_file = os.path.join(base_dir,'network_stats_for_filtering.tsv')

# Outcome: log(betw*10^6)
network_calculations_dir = os.path.join(base_dir,'network_calculations')

# Covariates about users and their neighbors
neighbor_country_info_dir = os.path.join(base_dir,'network_subset_country_info_no_rt')
neighbor_language_info_dir = os.path.join(base_dir,'network_subset_language_info_no_rt')
user_metadata_dir = os.path.join(base_dir,'user_metadata_no_rt')

out_dir = os.path.join(base_dir,'betweenness_dataframes')
if not os.path.exists(out_dir):
    os.mkdir(out_dir)

def get_eligible_pairs(network_stats_file):
    df = pd.read_csv(network_stats_file,sep='\t')
    df1 = df[(df['bilingual'] >= 20) & (df['monolingual c1'] >= 100) & (df['monolingual c2'] >= 100) & (df['bilingual neighbor'] >= 100)]
    country_pairs = [ast.literal_eval(c) for c in df1['country pair']]
    return country_pairs


def load_betweenness(network_calculations_dir,country_pair):
    filename = os.path.join(network_calculations_dir,f'{country_pair[0]}_{country_pair[1]}.tsv')
    df = pd.read_csv(filename,sep='\t',dtype=str)
    for column in df.columns:
        if column != 'uid':
            df[column] = df[column].astype(float)
    return df

def load_neighbor_info(neighbor_country_info_dir,neighbor_language_info_dir,country_pair):
    neighbor_country_filename = os.path.join(neighbor_country_info_dir,f'{country_pair[0]}_{country_pair[1]}.gz')
    neighbor_language_filename = os.path.join(neighbor_language_info_dir,f'{country_pair[0]}_{country_pair[1]}.gz')
    dfc = pd.read_json(neighbor_country_filename,lines=True,compression='gzip',dtype=str)
    dfl = pd.read_json(neighbor_language_filename,lines=True,compression='gzip',dtype=str)
   
    dfc = dfc[['id_str','num_neighbors_same_country','num_neighbors_other_country',
    'has_neighbors_same_country','has_neighbors_other_country']]
    dfl = dfl[['id_str','country','monolingual_majority','monolingual_minority','is_bilingual','num_neighbors',
    'num_neighbors_monolingual','num_neighbors_bilingual','has_monolingual_neighbor','has_bilingual_neighbor']]

    df = dfl.merge(dfc,on='id_str')
    for column in df.columns:
        if column not in ['country','id_str']:
            df[column] = df[column].astype(float)
    return df




def load_user_metadata(user_metadata_dir,country_pair):
    c1_filename = os.path.join(user_metadata_dir,f'{country_pair[0]}.gz')
    c2_filename = os.path.join(user_metadata_dir,f'{country_pair[1]}.gz')

    df1 = pd.read_json(c1_filename,lines=True,compression='gzip',dtype='str')
    df2 = pd.read_json(c2_filename,lines=True,compression='gzip',dtype='str')
    df = pd.concat([df1,df2])
    df = df.drop(columns=['hashtags','domains','lang','lang_norm'])
    df['verified'] = [1 if x == 'True' else 0 for x in df['verified']]

    for column in df.columns:
        if column not in ['country','id_str','verified']:
            df[column] = df[column].astype(float)
    return df


def combine_all_info(country_pair):
    print(country_pair)
    df_betw = load_betweenness(network_calculations_dir,country_pair)
    df_neighbor = load_neighbor_info(neighbor_country_info_dir,neighbor_language_info_dir,country_pair)
    df = df_neighbor.merge(df_betw,left_on='id_str',right_on='uid').drop(columns=['uid','degree'])
    df_user = load_user_metadata(user_metadata_dir,country_pair)
    df = df.merge(df_user,on=['id_str','country'])
    write_dataframes(df,country_pair)

def write_dataframes(df,country_pair):
    country_pair_out_dir = os.path.join(out_dir,f'{country_pair[0]}_{country_pair[1]}')
    if not os.path.exists(country_pair_out_dir):
        os.mkdir(country_pair_out_dir)
    for country in country_pair:
        out_file = os.path.join(country_pair_out_dir,f'{country}.tsv')
        df_country = df[df['country']==country]
        df_country.to_csv(out_file,index=False,sep='\t')
    






def main():
    country_pairs = get_eligible_pairs(network_stats_file)
    pool = Pool(12)
    pool.map(combine_all_info,country_pairs)



if __name__ == "__main__":
    main()
