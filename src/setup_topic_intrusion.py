from collections import defaultdict,Counter
import os
from typing import Hashable
import ujson as json
import glob
import re
import pandas as pd
import random 
import csv

base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
country_lang_file = os.path.join(base_dir,'country_languages.tsv')
language_set = sorted(list(set(pd.read_csv(country_lang_file,sep='\t')['language'])))
topic_model_data_dir = os.path.join(base_dir,'topic_model_data')
multilingual_text_dir = os.path.join(topic_model_data_dir,'multilingual_with_top100_hashtags') 
multilingual_topic_dir = os.path.join(topic_model_data_dir,f'multilingual_with_top100_hashtags_50topics_1Mtrain')
top_temporal_hashtag_file_by_lang = os.path.join(topic_model_data_dir,'all_top100_hashtags_by_lang.json')
hashtags_by_topic_file = os.path.join(topic_model_data_dir,f'top100_hashtags_by_topic_50topics_1M.json')
temporal_dirs = sorted(glob.glob(os.path.join(base_dir,'temporal_hashtags_no_rt','*','language','log_odds')))
eval_topics_dir = os.path.join(topic_model_data_dir,'eval')
task_dir = os.path.join(eval_topics_dir,'task')
true_dir = os.path.join(eval_topics_dir,'true')
for eval_dir in [eval_topics_dir,task_dir,true_dir]:
    if not os.path.exists(eval_dir):
        os.mkdir(eval_dir)




def get_all_top_hashtags_by_lang(temporal_dirs,top_temporal_hashtag_dir,num_hashtags):
    top_hashtags = defaultdict(list)
    for language in language_set:
        for temporal_dir in temporal_dirs:
            print(temporal_dir,language)
            filename = os.path.join(temporal_dir,language+'.tsv')
            try:
                df = pd.read_csv(filename,sep='\t',header=None)
                df.columns = ['name','z-score']
                top = list(df[df['z-score'] > 1.96]['name'])
                if len(top) > num_hashtags:
                    top = top[:num_hashtags]
                top_hashtags[language] += top
            except:
                continue
    with open(top_temporal_hashtag_file_by_lang,'w') as f:
        json.dump(top_hashtags,f)

def sample_topic_hashtags_by_language(top_temporal_hashtag_file_by_lang,
    hashtags_by_topic_file,
    num_samples_per_topic,
    lang_list):
    weighted_topic_hashtags = defaultdict(list)
    dfs = []
    with open(top_temporal_hashtag_file_by_lang,'r') as f:
        lang_hashtags = json.load(f)
    with open(hashtags_by_topic_file,'r') as f:
        topic_hashtags = json.load(f)
        for topic in topic_hashtags:
            df_topic = pd.DataFrame(topic_hashtags[topic],columns=['Frequency','Hashtag'])
            df_topic['Topic'] = topic
            dfs.append(df_topic)

    df = pd.concat(dfs)
    for lang in lang_list:
        df_lang = df[df['Hashtag'].isin(lang_hashtags[lang])]
        for topic in topic_hashtags:
            print(lang,topic)
            df_lang_topic = df_lang[df_lang['Topic']==topic]
            df_lang_other = df_lang[df_lang['Topic']!=topic]


            task_filename = os.path.join(task_dir,f'{lang}_{topic}.txt')
            true_filename = os.path.join(true_dir,f'{lang}_{topic}.txt')
            all_sampled_hashtags = []
            all_other_hashtags = []

            for _ in range(num_samples_per_topic):
                try:
                    sampled_topic_hashtags = list(df_lang_topic.sample(n=4,weights = df_lang_topic['Frequency'])['Hashtag'])
                    sampled_other_hashtags = list(df_lang_other.sample(n=1,weights = df_lang_other['Frequency'])['Hashtag'])
                    sampled_hashtags = random.sample(sampled_topic_hashtags + sampled_other_hashtags,5)
                    all_sampled_hashtags.append(sampled_hashtags)
                    all_other_hashtags.append(sampled_other_hashtags[0])
                except:
                    continue
            df_task = pd.DataFrame(all_sampled_hashtags)
            df_true = pd.DataFrame(all_other_hashtags)
            df_task.to_csv(task_filename,sep='\t')
            df_true.to_csv(true_filename,sep='\t')
                


def make_single_file_per_lang(task_dir,true_dir,lang_list,topic_list):
    for lang in lang_list:
        print(lang)
        dfs = []
        for topic in topic_list:
            task_filename = os.path.join(task_dir,f'{lang}_{topic}.txt')
            true_filename = os.path.join(true_dir,f'{lang}_{topic}.txt')
            df = pd.read_csv(task_filename,sep='\t').drop(columns=['Unnamed: 0'])
            df['true'] = pd.read_csv(true_filename,sep='\t').drop(columns=['Unnamed: 0'])
            df['topic'] = topic
            df['lang'] = lang
            dfs.append(df)
        df = pd.concat(dfs)
        df = df.sample(frac=1).reset_index(drop=True)
        df.to_csv(os.path.join(true_dir,lang+'.tsv'),sep='\t')
        df_task = df.drop(columns=['true','topic','lang'])
        df_task.to_csv(os.path.join(task_dir,lang+'.tsv'),sep='\t')



def main():
    #get_all_top_hashtags_by_lang(temporal_dirs,top_temporal_hashtag_file_by_lang,100)
    num_samples_per_topic = 5
    #lang_list = ['en','de','es','tr','it']
    lang_list = ['de','es','tr','it']
    entertainment_topics = ['1','10','19','24','44']
    promotion_topics = ['3','16','31']
    politics_topics = ['11','23','30','41','46','47']
    sports = ['48']
    topic_list = entertainment_topics + promotion_topics + sports + politics_topics

    sample_topic_hashtags_by_language(top_temporal_hashtag_file_by_lang,hashtags_by_topic_file,
    num_samples_per_topic,lang_list)
    make_single_file_per_lang(task_dir,true_dir,lang_list,topic_list)



if __name__ == "__main__":
    main()