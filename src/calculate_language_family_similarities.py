import pandas as pd 
import scipy.spatial
import json
import numpy as np 
from itertools import combinations 

euro_langs = list(pd.read_csv('country_languages.tsv',sep='\t')['language'])

with open('language_info.json','r') as f:
	family_info = json.load(f)

df = pd.DataFrame(family_info).transpose().reset_index()

df = df.drop(columns = ['index','alpha3-b','features'])
df = df[df['alpha2'].notna()]
print(df)
print(sorted(df['alpha2']))
families = df.set_index('alpha2').to_dict(orient='index')

euro_lang_pairs = list(combinations(euro_langs,2))
print(len(euro_lang_pairs))
all_sims = []
for pair in euro_lang_pairs:
	l1,l2 = sorted(pair)
	if l1 in families and l2 in families:
		f1 = set(families[l1]['families'])
		f2 = set(families[l2]['families'])
		similarity = len(f1.intersection(f2)) / len(f1.union(f2))
		all_sims.append((l1,l2,similarity))
	else:
		print(pair)

# all_sims_sorted = sorted(list(set(all_sims)),key=lambda x: x[-1])
# for pair in all_sims_sorted:
# 	print(pair)

# result = pd.DataFrame(all_sims_sorted,columns=['Lang 1','Lang 2', 'Lang Pair', 'Similarity'])
# print(result)
# result.to_csv('/Users/juliame/cross-lingual-exchange/country_language_metadata/euro_language_pair_similarities.tsv',sep='\t',index=False)
# result.to_csv('euro_language_pair_family_distances.tsv')