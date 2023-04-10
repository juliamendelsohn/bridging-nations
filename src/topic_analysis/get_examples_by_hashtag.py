import glob
import gzip
import os
import csv
import ujson as json
from collections import defaultdict,Counter
import pandas as pd
import random

def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())


def get_hashtags_from_tweet_obj(tweet):
    hashtags = set([h['text'].lower() for h in tweet['entities']['hashtags']])
    hashtags_extended = set()
    if 'extended_tweet' in tweet:
        hashtags_extended = set([h['text'].lower() for h in tweet['extended_tweet']['entities']['hashtags']])
    hashtags = hashtags.union(hashtags_extended)
    return list(hashtags)

def get_text_from_tweet_obj(tweet):
    if 'extended_tweet' in tweet:
        return tweet['extended_tweet']['full_text'].replace('\n',' ').replace('\t','')
    else:
        return tweet['text'].replace('\n',' ').replace('\t','')

def get_media_from_tweet_obj(tweet):
    links = []
    if 'urls' in tweet['entities']:
        links += [u['expanded_url'] for u in tweet['entities']['urls']]
    if 'media' in tweet['entities']:
        links += [u['media_url'] for u in tweet['entities']['media']]
    if 'extended_tweet' in tweet:
        if 'urls' in tweet['extended_tweet']['entities']:
            links += [u['expanded_url'] for u in tweet['extended_tweet']['entities']['urls']]
        if 'media' in tweet['entities']:
            links += [u['media_url'] for u in tweet['extended_tweet']['entities']['media']]
    return list(set(links))
    

def get_tweets_with_hashtags(filename,languages,eval_hashtags,num_samples,num_collected,example_tweets,out_dir):
    with gzip.open(filename,'r') as f:
        for i,line in enumerate(f):
            tweet = load_tweet_obj(line)
            if not tweet['text'].startswith('RT'):
                lang = tweet['lang']
                if lang in languages: 
                    hashtags = get_hashtags_from_tweet_obj(tweet)
                    for hashtag in hashtags:
                        if hashtag in eval_hashtags[lang] and num_collected[lang][hashtag] < num_samples:
                            text = get_text_from_tweet_obj(tweet)
                            links = get_media_from_tweet_obj(tweet)
                            num_collected[lang][hashtag] += 1
                            example_tweets[lang].append([hashtag,text,links])
                            with open(os.path.join(out_dir,lang+'_examples.tsv'),'a') as f:
                                writer = csv.writer(f,delimiter='\t')
                                writer.writerow([hashtag,text,links])
                            break
            if all(all(num_collected[lang][h] >= num_samples for h in list(eval_hashtags[lang])) for lang in languages):
                return
            if i % 10000 == 0:
                print(filename,i)


def write_entries(out_dir,example_tweets):
    for lang in example_tweets:
        out_file = os.path.join(out_dir,lang + '.tsv')
        df = pd.DataFrame(example_tweets[lang])
        df.columns = ['Hashtag','Example Tweet','Links']
        df = df.sort_values(by='Hashtag')
        df.to_csv(out_file,sep='\t',index=False)

def get_hashtags_for_eval(eval_task_dir,languages):
    eval_hashtags = {}
    for lang in languages:
        filename = os.path.join(eval_task_dir,lang+'.tsv')
        task = pd.read_csv(filename,sep='\t')
        task = task.drop(columns='Unnamed: 0')
        hashtag_list = []
        for colname,col in task.iteritems():
            hashtag_list += list(col)
        eval_hashtags[lang] = set(hashtag_list)
    return eval_hashtags

def main():

    tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
    eval_task_dir = "/shared/2/projects/cross-lingual-exchange/data/topic_model_data/eval/task"
    out_dir = "/shared/2/projects/cross-lingual-exchange/data/topic_model_data/eval/examples"
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    languages = ['es','de','tr','it']
    eval_hashtags = get_hashtags_for_eval(eval_task_dir,languages)

    all_tweet_files = glob.glob(os.path.join(tweet_dir,'*.gz'))
    all_tweet_files = random.sample(all_tweet_files,len(all_tweet_files))
    all_tweet_files = all_tweet_files
        
        
    num_collected = defaultdict(lambda: defaultdict(int))
    example_tweets = defaultdict(list)
    num_samples = 10
    for filename in all_tweet_files:
        get_tweets_with_hashtags(filename,languages,eval_hashtags,num_samples,num_collected,example_tweets,out_dir)
        if all(all(num_collected[lang][h] >= num_samples for h in list(eval_hashtags[lang])) for lang in languages):
            print('complete')
            break
    #write_entries(out_dir,example_tweets)


    
    
if __name__ == "__main__":
    main()
