""" 
- Loads betweenness datasheet 
- For every (c1,c2) in country_pair with majority langs L1, L2
- Get # hashtags in L1, # hashtags in L2, and percent of total hashtags with identifiable language
- new dataframe with columns (for all: hashtags, domains, temporal_hashtags): 
- uid
- has_hashtag_other_country_lang
- has_hashtag_same_country_lang
- num_hashtag_other_country_lang
- num_hashtag_same_country_lang
- join with betweenness datasheet on uid
- compress each datasheet to .gzip
- multilingual_friend_effect_data

-base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
network_stats_file = os.path.join(base_dir,'network_stats_for_filtering.tsv')
- country_lang_file = os.path.join(base_dir,'country_languages.tsv')


out_dir = os.path.join(base_dir,'user_language_intersections_no_rt')
hashtag_out_dir = os.path.join(out_dir,'hashtags')
domain_out_dir = os.path.join(out_dir,'domains')
temporal_out_dir = os.path.join(out_dir,'temporal_hashtags')

"""
import os
import pandas as pd 
import json
from collections import defaultdict
from multiprocessing import Pool
import random
import tqdm


base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
country_lang_file = os.path.join(base_dir,'country_languages.tsv')
betweenness_dataframes_dir = os.path.join(base_dir,'betweenness_dataframes')
multilingual_friend_effect_dataframes_dir = os.path.join(base_dir,'multilingual_friend_effect_dataframes_top100')
country_codes_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
country_lang_file = '/shared/2/projects/cross-lingual-exchange/data/country_languages.tsv'
user_hashtag_domain_by_lang = os.path.join(base_dir,'user_language_intersections')
#hashtag_use_by_lang = os.path.join(user_hashtag_domain_by_lang,'hashtags')
domain_use_by_lang = os.path.join(user_hashtag_domain_by_lang,'domains_top100')
temporal_use_by_lang = os.path.join(user_hashtag_domain_by_lang,'temporal_hashtags_top100')
country_to_lang = pd.read_csv(country_lang_file,sep='\t').set_index('country').to_dict()['language']
#info_types = ['hashtags','domains','temporal_hashtags']
info_types = ['domains_top100','temporal_hashtags_top100']
num_topics=50

if not os.path.exists(multilingual_friend_effect_dataframes_dir):
    os.mkdir(multilingual_friend_effect_dataframes_dir)

def add_user_other_lang_info(country_pair):
    for country in country_pair.split('_'):
        print(country_pair, country)
        betweenness_file = os.path.join(betweenness_dataframes_dir,country_pair,country+ '.tsv')
        df_betw = pd.read_csv(betweenness_file, sep = '\t', dtype = str)
        (c1,c2) = country_pair.split('_')
        other_country = c1 if c1 != country else c2
        lang = country_to_lang[country]
        other_lang = country_to_lang[other_country]

        user_info_sharing_features = defaultdict(lambda: defaultdict(int))
        for info_type in info_types:
            info_file = os.path.join(user_hashtag_domain_by_lang,info_type,country + '.json')
            with open(info_file,'r') as f:
                info_dict = json.load(f)
                for uid in df_betw['id_str']:
                    #user_info_sharing_features[uid][f'has_{info_type}_same_language'] = 0
                    #user_info_sharing_features[uid][f'num_{info_type}_same_language'] = 0
                    user_info_sharing_features[uid][f'has_{info_type}_other_language'] = 0
                    #user_info_sharing_features[uid][f'num_{info_type}_other_language'] = 0
                    
                    if uid in info_dict:
                        #if lang in info_dict[uid]:
                        #    user_info_sharing_features[uid][f'has_{info_type}_same_language'] = 1
                        #    user_info_sharing_features[uid][f'num_{info_type}_same_language'] = info_dict[uid][lang]
                        if other_lang in info_dict[uid]:
                            user_info_sharing_features[uid][f'has_{info_type}_other_language'] = 1
                            #user_info_sharing_features[uid][f'num_{info_type}_other_language'] = info_dict[uid][other_lang]

            if info_type.startswith('temporal_hashtag'):
                temporal_hashtag_topic_file = os.path.join(user_hashtag_domain_by_lang,info_type,country + '_with_topic.json')
                with open(temporal_hashtag_topic_file,'r') as f:
                    topic_dict = json.load(f)
                    for uid in df_betw['id_str']:
                        for i in range(num_topics):
                            #user_info_sharing_features[uid][f'has_{info_type}_same_language_topic_{i}'] = 0
                            #user_info_sharing_features[uid][f'num_{info_type}_same_language_topic_{i}'] = 0
                            user_info_sharing_features[uid][f'has_{info_type}_other_language_topic_{i}'] = 0
                            #user_info_sharing_features[uid][f'num_{info_type}_other_language_topic_{i}'] = 0
                        if uid in topic_dict:
                            # if lang in topic_dict[uid]:
                            #     for i in range(num_topics):
                            #         if str(i) in topic_dict[uid][lang]:
                            #             user_info_sharing_features[uid][f'has_{info_type}_same_language_topic_{i}'] = 1
                            #             user_info_sharing_features[uid][f'num_{info_type}_same_language_topic_{i}'] = topic_dict[uid][lang][str(i)]
                            if other_lang in topic_dict[uid]:
                                for i in range(num_topics):
                                    if str(i) in topic_dict[uid][other_lang]:
                                        user_info_sharing_features[uid][f'has_{info_type}_other_language_topic_{i}'] = 1
                                        #user_info_sharing_features[uid][f'num_{info_type}_other_language_topic_{i}'] = topic_dict[uid][other_lang][str(i)]



        df_info = pd.DataFrame.from_dict(user_info_sharing_features, orient='index')
        df_info.index.name = 'id_str'
        df_info = df_info.reset_index()
        df = df_betw.merge(df_info,on='id_str')
        out_dir_country_pair = os.path.join(multilingual_friend_effect_dataframes_dir,country_pair)
        if not os.path.exists(out_dir_country_pair):
            os.mkdir(out_dir_country_pair)
        out_file = os.path.join(out_dir_country_pair,country + '.gz')
        df.to_csv(out_file,sep='\t',compression='gzip')


def main():
    pool = Pool(12)
    country_pairs = os.listdir(betweenness_dataframes_dir)
    random.shuffle(country_pairs)
    #pool.map(add_user_other_lang_info,country_pairs)
    # for _ in tqdm.tqdm(pool.imap_unordered(add_user_other_lang_info,country_pairs),total=len(country_pairs)):
    #     pass
    

if __name__ == "__main__":
    main()