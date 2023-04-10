from itertools import combinations
import pandas as pd
from collections import Counter

countries_and_languages_file = '/Users/juliame/cross-lingual-exchange/country_language_metadata/country_languages.tsv'

western_romance = set(['ca','es','pt','it','fr'])
west_germanic = set(['de','en','nl'])
scandinavian = set(['da','no','sv','is'])
baltic = set(['lt','lv'])
slavic = set(['ru','pl','bg','cs','sl'])
finnic = set(['et','fi'])

balto_slavic = baltic | slavic
germanic = west_germanic | scandinavian
romance = western_romance | set(['ro'])


indo_european = balto_slavic | germanic | romance | {'hy','el'}
uralic = finnic | {'hu'}
other = {'iw','ka','tr'}

languages = list(set(pd.read_csv(countries_and_languages_file,sep='\t')['language']))
language_pairs = list(combinations(languages,2))
results = []

for pair in language_pairs:
	l1 = pair[0]
	l2 = pair[1]
	closeness = 0
	for primary_family in [indo_european,uralic]:
		if l1 in primary_family and l2 in primary_family:
			closeness = 1
	for family in [balto_slavic,germanic,romance]:
		if l1 in family and l2 in family:
			closeness = 2
	for subfamily in [western_romance,west_germanic,scandinavian,baltic,slavic,finnic]:
		if l1 in subfamily and l2 in subfamily:
			closeness = 3
	results.append((l1,l2,closeness))
	results.append((l2,l1,closeness))

df = pd.DataFrame(results,columns=['language','language_other','linguistic_closeness'])
df.to_csv('/Users/juliame/cross-lingual-exchange/country_language_metadata/language_pairs_family_features.tsv',sep='\t',index=False)
