import pandas as pd 
import glob
import os




data_dir = '/Users/juliame/cross-lingual-exchange/multilingual_friend_effect_dataframes_with_rt/'
out_file = '/Users/juliame/cross-lingual-exchange/bilingual_counts.tsv'


results = []
for country_pair in os.listdir(data_dir):
	for country in country_pair.split('_'):
		filename = os.path.join(data_dir,country_pair,country + '.gz')
		df = pd.read_csv(filename,sep='\t',compression='gzip')
		num_bilinguals = df['is_bilingual'].sum()
		num_with_bilingual_friend = df['has_bilingual_neighbor'].sum()
		print(country_pair,country,num_bilinguals,num_with_bilingual_friend)
		results.append((country_pair,country,num_bilinguals,num_with_bilingual_friend))

res = pd.DataFrame(results,columns=['country_pair','country','bilingual_count','bilingual_neighbor_count'])
res.to_csv(out_file,sep='\t',index=False)
