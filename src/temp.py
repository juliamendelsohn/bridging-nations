import os
import glob

tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
year_range = range(2018,2021)
all_tweet_files = []
for year in year_range:
    for m in range(1,13):
        month = str(m)
        if len(month) == 1:
            month = '0' + month
        for d in range(1,32):
            day = str(d)
            if len(day) == 1:
                day = '0' + day
            tweet_files = glob.glob(os.path.join(tweet_dir,f'decahose.{year}-{month}-{day}*.gz'))
            if len(tweet_files) == 0:
                tweet_files = glob.glob(os.path.join(tweet_dir,f'decahose.{year}{month}{day}*.gz'))
            all_tweet_files += tweet_files
print(len(all_tweet_files[::-1]))