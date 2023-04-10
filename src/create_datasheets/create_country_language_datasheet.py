import pandas as pd 
import scipy.spatial
import json
import numpy as np 
from itertools import combinations 
from collections import Counter
import os
# Language datasheet
# Language similarity


# Country Datasheet
# population country 1
# population country 2
# percent foreign born country 1
# percent foreign born country 2
# migration from country 1 to country 2
# migration from country 2 to country 1
#geographic distance from country 1 to country 2
# official common language (comlang_off)
# non-official widely spoken common language (comlang_ethno)
# historically same country (smctry)
# dist
# contig
# iso_o iso_d  contig  comlang_off  comlang_ethno  colony  comcol  curcol  col45  smctry          dist       distcap         distw      distwces

project_dir = '/shared/2/projects/cross-lingual-exchange/'
data_dir = os.path.join(project_dir,'data','country_language_metadata')
effects_dir = os.path.join(project_dir,'results')

european_countries_file = os.path.join(data_dir,'european_country_codes_with_alpha3.tsv')
countries_and_languages_file = os.path.join(data_dir,'country_languages.tsv')
total_pop_file = os.path.join(data_dir,'population_world_bank.csv')
percent_migrant_file = os.path.join(data_dir,'percent_migrant_stock_world_bank.csv')
language_similarity_file = os.path.join(data_dir,'language_pairs_family_features.tsv')
migration_dyads_file = os.path.join(data_dir,'migration_dyads_oecd.csv')
migration_dyads_file = os.path.join(data_dir,'bilateral_migration_long_2017.csv')
geo_dist_file = os.path.join(data_dir,'geo_dist.tsv')
gravity_file = os.path.join(data_dir,'gravity_europe.tsv')
conflicts_file = os.path.join(data_dir,'gdelt_conflicts_europe.tsv')
out_file = os.path.join(effects_dir,'effects_with_country_vars_11-08-21.tsv')

effect_files = {}
effect_files['betweenness'] = 'att_betweenness_11-02-2021.tsv'
effect_files['domain'] = 'att_domains_top100_with_rt_11-02-2021.tsv'
effect_files['hashtag'] = 'att_temporal_hashtags_top100_with_rt_11-02-2021.tsv'
effect_files['entertainment'] = 'entertainment_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['politics'] = 'politics_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['sports'] = 'sports_att_temporal_hashtag_top100_with_rt.tsv'
effect_files['promotion'] = 'promotion_att_temporal_hashtag_top100_with_rt.tsv'


for i,outcome in enumerate(effect_files):
	filename = os.path.join(effects_dir,effect_files[outcome])
	print(filename)
	new_df = pd.read_csv(filename,sep='\t')
	print(new_df)
	new_df = new_df[new_df['Country'] != 'all']
	new_df[outcome] = new_df['Estimate']
	new_df = new_df[['Country Pair', 'Country', outcome]]

	if i == 0:
		df_effects = new_df
	else:
		df_effects = df_effects.merge(new_df,on=['Country Pair','Country'],how='outer')
print(df_effects)



other_country = []
for i,row in df_effects.iterrows():
	country_pair = row['Country Pair']
	country = row['Country']
	if country == country_pair.split('_')[0]:
		other_country.append(country_pair.split('_')[1])
	else:
		other_country.append(country_pair.split('_')[0])
df_effects['Other Country'] = other_country
print(df_effects.shape)


df_country_codes = pd.read_csv(european_countries_file,sep='\t')
df_country_to_lang = pd.read_csv(countries_and_languages_file,sep='\t')
df = df_country_codes.merge(df_country_to_lang,left_on='alpha2',right_on='country')

df_pop = pd.read_csv(total_pop_file)[['Country Code','2018']]
df_pop.columns = ['alpha3','total_pop_2018']
df = df.merge(df_pop,on='alpha3')

# df_migrant_percent = pd.read_csv(percent_migrant_file)[['Country Code','2015']]
# df_migrant_percent.columns = ['alpha3','percent_migrant_2015']
# df = df.merge(df_migrant_percent,on='alpha3')

country_to_info = df.set_index('alpha2').to_dict(orient='index')
df_combined = df_effects.merge(df,left_on='Country',right_on='country').drop(columns='name')

df_other_combined = df_effects[['Country Pair','Country','Other Country']].merge(df.drop(columns=['country']),left_on='Other Country',right_on='alpha2').drop(columns='name')
df = df_combined.merge(df_other_combined,on=['Country Pair','Country'],suffixes=('','_other'))

# df_migration_dyads = pd.read_csv(migration_dyads_file)
# df_mig_dyads = df_migration_dyads[(df_migration_dyads['Variable']=='Stock of foreign-born population by country of birth') & (df_migration_dyads['Year']==2018) & (df_migration_dyads['Gender']=='Total')]
# df_mig_dyads = df_mig_dyads[['CO2','COU','Value']]
# df_mig_dyad_orig = df_mig_dyads[['CO2','COU','Value']]
# df_mig_dyad_orig.columns = ['alpha3','alpha3_other','num_migrants_country_to_other']
# df_mig_dyad_orig_other = df_mig_dyads[['CO2','COU','Value']]
# df_mig_dyad_orig_other.columns = ['alpha3_other', 'alpha3', 'num_migrants_other_to_country']

# df = df.merge(df_mig_dyad_orig,on=['alpha3','alpha3_other'],how='left')
# df = df.merge(df_mig_dyad_orig_other,on=['alpha3','alpha3_other'],how='left')
# df['percent_migrants_country_to_other'] = df['num_migrants_country_to_other'] / df['total_pop_2018_other']
# df['percent_migrants_other_to_country'] = df['num_migrants_other_to_country'] / df['total_pop_2018']

df_migrant = pd.read_csv(migration_dyads_file)
df_migrant = df_migrant.replace(['Czech Republic'],['Czechia'])
df_migrant = df_migrant.drop(columns=['Unnamed: 0'])
print(df_country_codes)
print(df_migrant)
df_country_codes_bilateral = df_country_codes
df_country_codes_bilateral['alpha3_source'] = df_country_codes['alpha3']
df_country_codes_bilateral['alpha3_destination'] = df_country_codes['alpha3']
df_migrant = df_migrant.merge(df_country_codes_bilateral[['name','alpha3_source']],right_on=['name'],left_on=['Source'])
#df_migrant.columns = ['Source','Destination','']
df_migrant = df_migrant.merge(df_country_codes_bilateral[['name','alpha3_destination']],right_on=['name'],left_on=['Destination'])
df_migrant = df_migrant.drop(columns=['name_x','name_y'])
print(df_migrant)

df_migrants_from_source = pd.read_csv(migration_dyads_file)
df_migrants_from_source = df_migrants_from_source.replace(['Czech Republic'],['Czechia'])
df_migrants_from_source = df_migrants_from_source[df_migrants_from_source['Destination']=='World']
df_migrants_from_source = df_migrants_from_source.merge(df_country_codes[['name','alpha3']],right_on=['name'],left_on=['Source'])
df_migrants_from_source = df_migrants_from_source[['alpha3','Migrants']]
df_migrants_from_source = df_migrants_from_source.rename(columns={'Migrants':'migrants_from_source'})


df_migrants_to_dest = pd.read_csv(migration_dyads_file)
df_migrants_to_dest = df_migrants_to_dest.replace(['Czech Republic'],['Czechia'])
df_migrants_to_dest = df_migrants_to_dest[df_migrants_to_dest['Source']=='World']
df_migrants_to_dest = df_migrants_to_dest.merge(df_country_codes[['name','alpha3']],right_on=['name'],left_on=['Destination'])
df_migrants_to_dest = df_migrants_to_dest[['alpha3','Migrants']]
df_migrants_to_dest = df_migrants_to_dest.rename(columns={'Migrants':'migrants_to_dest'})


df = df.merge(df_migrant,left_on=['alpha3','alpha3_other'],right_on=['alpha3_source','alpha3_destination'],how='left')
df = df.rename(columns={'Migrants':'num_migrants_c1_c2'})
df = df.merge(df_migrant,left_on=['alpha3_other','alpha3'],right_on=['alpha3_source','alpha3_destination'],how='left')
df = df.rename(columns={'Migrants':'num_migrants_c2_c1'})
df = df.drop(columns=['Source_x','Source_y','Destination_x','Destination_y','alpha3_source_x',
'alpha3_source_y','alpha3_destination_x','alpha3_destination_y'])

df_migrants_to_c1 = df_migrants_to_dest.rename(columns={'migrants_to_dest':'num_migrants_dest_c1'})
df_migrants_to_c2 = df_migrants_to_dest.rename(columns={'migrants_to_dest':'num_migrants_dest_c2'})
df_migrants_from_c1 = df_migrants_from_source.rename(columns={'migrants_from_source':'num_migrants_source_c1'})
df_migrants_from_c2 = df_migrants_from_source.rename(columns={'migrants_from_source':'num_migrants_source_c2'})
df = df.merge(df_migrants_to_c1,on=['alpha3'],how='left')
df = df.merge(df_migrants_to_c2,left_on=['alpha3_other'],right_on=['alpha3'],how='left')
df = df.rename(columns={'alpha3_x':'alpha3'})
df = df.drop(columns=['alpha3_y'])
df = df.merge(df_migrants_from_c1,on=['alpha3'],how='left')
df = df.merge(df_migrants_from_c2,left_on=['alpha3_other'],right_on=['alpha3'],how='left')
df = df.rename(columns={'alpha3_x':'alpha3'})
df = df.drop(columns=['alpha3_y'])


#Counts of migrants from c1 and from c2 (num_migrant_source_c1,num_migrant_source_c2) (norm by pop of source?)
#Counts of migrants to c1 and to c2  (num_migrant_dest_c1, num_migrant_dest_c2) (norm by pop of dest?)
#Percent of migrants in c2 from c1 (num_migrants_c1_c2 / num_migrant_dest_c2 )
#Percent of migrants in c1 from c2 (num_migrants_c2_c1 / num_migrant_dest_c1)
df['percent_migrants_source_c1'] = df['num_migrants_source_c1'] / df['total_pop_2018']
df['percent_migrants_source_c2'] = df['num_migrants_source_c2'] / df['total_pop_2018_other']
df['percent_migrants_dest_c1'] = df['num_migrants_dest_c1'] / df['total_pop_2018']
df['percent_migrants_dest_c2'] = df['num_migrants_dest_c2'] / df['total_pop_2018_other']
df['percent_c2_migrants_from_c1'] = df['num_migrants_c1_c2'] / df['num_migrants_dest_c2']
df['percent_c1_migrants_from_c2'] = df['num_migrants_c2_c1'] / df['num_migrants_dest_c1']


df['population_difference'] = df['total_pop_2018'] - df['total_pop_2018_other']
df['population_ratio'] = df['total_pop_2018'] / df['total_pop_2018_other']
print(df.shape)

df_dist = pd.read_csv(geo_dist_file,sep='\t')[['iso_o','iso_d','contig','smctry','dist']]
df_dist = df_dist.replace(['ROM'],['ROU'])
df_dist.columns = ['alpha3', 'alpha3_other', 'contiguous', 'historically_same_country','distance']
df = df.merge(df_dist,on=['alpha3','alpha3_other'],how='left')



gravity_cols = ['iso3_o','iso3_d','pop_o','pop_d','gdp_o','gdp_d','gdpcap_o','gdpcap_d','eu_o','eu_d','rta',
'tradeflow_imf_o','tradeflow_imf_d','gmt_offset_2020_o','gmt_offset_2020_d']
df_gravity = pd.read_csv(gravity_file,sep='\t')[gravity_cols]
df_gravity = df_gravity.replace(['ROM'],['ROU'])

df_gravity['tradeflow_country_to_other'] = (df_gravity['tradeflow_imf_o'] + df_gravity['tradeflow_imf_d'])/2
print(df_gravity)

df_trade = df_gravity[['iso3_o','iso3_d','tradeflow_country_to_other']]
df_trade.columns = ['alpha3', 'alpha3_other','tradeflow_country_to_other']
df_trade_other = df_gravity[['iso3_o','iso3_d','tradeflow_country_to_other']]
df_trade_other.columns = ['alpha3_other','alpha3','tradeflow_other_to_country']

df = df.merge(df_trade,on=['alpha3','alpha3_other'],how='left')
df = df.merge(df_trade_other,on=['alpha3','alpha3_other'],how='left')

gravity_cols = ['iso3_o','iso3_d','pop_o','pop_d','gdp_o','gdp_d','gdpcap_o','gdpcap_d','eu_o','eu_d','rta',
'tradeflow_imf_o','tradeflow_imf_d','gmt_offset_2020_o','gmt_offset_2020_d']

df_time_pop = df_gravity[['iso3_o','iso3_d','gmt_offset_2020_o','gmt_offset_2020_d','pop_o','pop_d']]
df_time_pop.columns = ['alpha3','alpha3_other','timezone','timezone_other','pop','pop_other']
df = df.merge(df_time_pop, on=['alpha3','alpha3_other'],how='left')

df_econ = df_gravity[['iso3_o','iso3_d','gdp_o','gdp_d','gdpcap_o','gdpcap_d','eu_o','eu_d','rta']]
df = df.merge(df_econ,left_on=['alpha3','alpha3_other'],right_on=['iso3_o','iso3_d'],how='left')

df_conflict = pd.read_csv(conflicts_file,sep='\t')
df_conflict = df_conflict.drop(columns=['Unnamed: 0'])
df = df.merge(df_conflict,on=['alpha3','alpha3_other'],how='left')

df_lang_sim = pd.read_csv(language_similarity_file,sep='\t')
#print(df_lang_sim.groupby(by=['language','language_other']).agg('mean'))
#print(df[['language','language_other']])
df = df.merge(df_lang_sim,on=['language','language_other'],how='inner')
df['linguistic_distance'] = max(df['linguistic_closeness']) - df['linguistic_closeness']

df['log_trade_c1_c2'] = np.log(df['tradeflow_country_to_other'])
df['log_trade_c2_c1'] = np.log(df['tradeflow_other_to_country'])
df['log_pop_o'] = np.log(df['pop'])
df['log_pop_d'] = np.log(df['pop_other'])
df['log_gdp_o'] = np.log(df['gdp_o'])
df['log_gdp_d'] = np.log(df['gdp_d'])
df['log_gdpcap_o'] = np.log(df['gdpcap_o'])
df['log_gdpcap_d'] = np.log(df['gdpcap_d'])
df['time_difference_c2_c1'] = df['timezone_other'] - df['timezone']
df['log_dist'] = np.log(df['distance'])


df.to_csv(out_file,sep='\t')
print(df)
print(df.columns)

