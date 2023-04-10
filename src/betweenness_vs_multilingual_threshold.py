import os
import pandas as pd
import csv
import numpy as np

def load_betweenness(network_calculations_dir,country_pair):
    dfs = []
    country_pair_str = f'{country_pair[0]}_{country_pair[1]}'
    for country in country_pair:
        filename = os.path.join(network_calculations_dir,country_pair_str,country+'.tsv')
        df = pd.read_csv(filename,sep='\t',dtype={'id_str':str})
        df['scaled_betw'] = df['betw'] * 1e6
        df['log_betw'] = np.log(df['scaled_betw']+1)
        dfs.append(df)
    return pd.concat(dfs)

def load_lang_prop(user_lang_prop_dir,country_pair):
    dfs = []
    for country in country_pair:
        filename = os.path.join(user_lang_prop_dir,country+'.gz')
        df = pd.read_json(filename,compression='gzip',lines=True,dtype={'id_str':str})
        dfs.append(df)
    return pd.concat(dfs).reset_index()

def get_country_pairs(country_pair_file):
    country_pairs = []
    with open(country_pair_file,'r') as f:
        reader = csv.reader(f)
        for row in reader: country_pairs.append(row)
    return country_pairs

#given lang_norm dict, at what threshold are they considered multilingual
#thresh = 0 --> just one language appears, no matter what than is_bilingual = TRUE
#if multiple languages are listed, exclude und, then what is the second highest percentage? 
def get_user_multilingual_threshold(lang_norm,l1,l2):
    threshold = 0
    if l1 in lang_norm and l2 in lang_norm:
        threshold = min(lang_norm[l1],lang_norm[l2])
    return threshold

def main():
    data_dir = '/shared/2/projects/cross-lingual-exchange/data/'
    out_dir = '/shared/2/projects/cross-lingual-exchange/results/'
    country_pair_file = os.path.join(data_dir,'country_pairs.csv')
    network_calculations_dir = os.path.join(data_dir,'betweenness_dataframes')
    user_lang_prop_dir = os.path.join(data_dir,'user_metadata_no_rt')
    country_pairs = get_country_pairs(country_pair_file)
    country_to_lang = pd.read_csv(os.path.join(data_dir,'country_languages.tsv'),sep='\t').set_index('country').to_dict()['language']
    res = []
    for i,country_pair in enumerate(country_pairs):
        print(i,country_pair)
        [l1,l2] = [country_to_lang[c] for c in country_pair]
        df_betw = load_betweenness(network_calculations_dir,country_pair)
        df_langs  = load_lang_prop(user_lang_prop_dir,country_pair)
        df_langs['multilingual_threshold'] = df_langs['lang_norm'].apply(get_user_multilingual_threshold,args=(l1,l2))
        df = df_betw.merge(df_langs,on='id_str')[['id_str','log_betw','multilingual_threshold']]
        res.append(df)
    res_df = pd.concat(res)
    print(res_df)
    res_df.to_csv(os.path.join(out_dir,'user_multilingual_thresholds.tsv'),sep='\t')

    threshold_betw = []
    thresh_list = [i/200 for i in range(101)]
    for thresh in thresh_list:
        mean_at_thresh = res_df[res_df['multilingual_threshold'] >= thresh]['log_betw'].mean()
        se_at_thresh = res_df[res_df['multilingual_threshold'] >= thresh]['log_betw'].sem()
        diff_at_thresh = res_df[res_df['multilingual_threshold'] >= thresh]['log_betw'].mean() - res_df[res_df['multilingual_threshold'] < thresh]['log_betw'].mean()
        threshold_betw.append((thresh,mean_at_thresh,se_at_thresh,diff_at_thresh))
    df_thresh = pd.DataFrame(threshold_betw,columns=['Threshold','Mean','Std. Error','Difference'])
    print(df_thresh)
    df_thresh.to_csv(os.path.join(out_dir,'betw_mean_vary_thresholds.tsv'),sep='\t')


if __name__ == "__main__":
    main()