import scipy.sparse as sparse
import numpy as np
import json
from itertools import compress
import pandas as pd

family_file = "/Users/juliame/Downloads/family_features.npz"
feature_file = "/Users/juliame/Downloads/feature_averages.npz"


language_codes = pd.read_csv("../language_codes.csv").set_index('alpha3-b')
language_codes = language_codes.drop(columns=['French','alpha3-t'])
language_codes = language_codes.to_dict(orient='index')

fam = np.load(family_file)
fam_data = list(fam.get('data'))
all_langs = list(fam.get('langs'))
all_families = list(fam.get('feats'))
lang_info = {}
features = np.load(feature_file)
all_features = list(features.get('feats'))
feat_data = list(features.get('data'))

for i in range(len(all_langs)):
	fil = fam_data[i]
	lang = all_langs[i]
	if lang in language_codes:
		alpha2 = language_codes[lang]['alpha2']
		name = language_codes[lang]['English']
		families = [x[2:] for x in compress(all_families,fil)]
		lang_info[name] = {}
		lang_info[name]['alpha2'] = alpha2
		lang_info[name]['alpha3-b'] = lang
		lang_info[name]['families'] = families
		lang_info[name]['features'] = list(feat_data[i].transpose()[0])

#print(lang_info)
with open('language_info.json','w') as f:
	json.dump(lang_info,f)
