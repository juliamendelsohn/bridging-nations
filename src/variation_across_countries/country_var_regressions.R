library('car')
library('stargazer')
library('broom')
library('MASS')

outcomes <- list('betweenness','domain','hashtag') 
date <- "1-11-2022"
models <- list()
for(i in 1:length(outcomes)){
  outcome <- outcomes[[i]]
  filename <- paste('/shared/2/projects/cross-lingual-exchange/results/',outcome,'_effects_with_country_vars_11-08-21_small.tsv',sep='')
  df <- read.table(filename,header=TRUE,sep='\t')
  df$outcome <- df[[paste(outcome,sep='')]]
  df[is.na(df)] <- 0
  df$pop_ratio <- df['pop'] / df['pop_other']
  df$gdp_ratio <- df['gdp_o'] / df['gdp_d']
  df$gdpcap_ratio <- df['gdpcap_o'] / df['gdpcap_d']
  df$c1_in_eu <- df$eu_o
  df$c2_in_eu <- df$eu_d
  df$pop_c1 <- df$pop 
  df$pop_c2 <- df$pop_other
  df$gdp_c1 <- df$gdp_o
  df$gdp_c2 <- df$gdp_d
  df$gdpcap_c1 <- df$gdpcap_o
  df$gdpcap_c2 <- df$gdpcap_d 
  df$tradeflow_c1_to_c2 <- df$tradeflow_country_to_other
  df$tradeflow_c2_to_c1 <- df$tradeflow_other_to_country
  df$num_conflicts <- (df$num_conflicts_c1_to_c2 + df$num_conflicts_c2_to_c1) / 2
  df$tradeflow <- (df$tradeflow_c1_to_c2 + df$tradeflow_c2_to_c1) /2
  df$log_tradeflow <- log(df$tradeflow)
  df$log_pop_ratio <- log(df$pop_c1 / df$pop_c2)
  df$tradeflowcap <- df$tradeflow / (df$pop_c1 + df$pop_c2)
  df$log_tradeflowcap <- log(df$tradeflowcap)
  df$both_eu_members <- df$c1_in_eu * df$c2_in_eu
  vars <- c(
    #'pop_c1','pop_c2',
    'distance', 
    'time_difference_c2_c1',
    'pop_ratio',
    'percent_c2_migrants_from_c1',
    'percent_c1_migrants_from_c2',
    'percent_migrants_dest_c1',
    'percent_migrants_dest_c2',
    #'gdp_c1','gdp_c2',
    #'gdp_ratio',
    #'gdpcap_c1','gdpcap_c2',
    'gdpcap_ratio',
    #'c1_in_eu','c2_in_eu',
    #'both_eu_members',
    'rta',
    #'tradeflow',
    'tradeflowcap',
    #'log_tradeflowcap',
    'percent_conflicts_c1_to_c2',
    'percent_conflicts_c2_to_c1',
    #'num_conflicts',
    'linguistic_distance'
    )
  #df.vars <- na.omit(df[vars]) 
  df[vars] <- scale(df[vars],scale=TRUE)
  df$outcome <- scale(df$outcome)
  #pc <- prcomp(df.vars,center=TRUE,scale=TRUE) 
  #print(pc)
  #print(summary(pc))
  #print(df[vars][!complete.cases(df[vars]),])
  #df[is.na(df)] <- 0
  z = cor(df[vars])
  z[lower.tri(z,diag=TRUE)]=NA  #Prepare to drop duplicates and meaningless information
  z=as.data.frame(as.table(z))  #Turn into a 3-column table
  z=na.omit(z)  #Get rid of the junk we flagged above
  z=z[order(-abs(z$Freq)),]    #Sort by highest correlation (whether +ve or -ve)
  #print(z)
  df$num_treated <- df['Treated']
  f <- as.formula(paste(outcome,paste(vars,collapse='+'),sep='~'))
  model <- lm(f,data=df,weights=df$Treated)
  print(vif(model))
  res <- tidy(model)
  print(summary(model))
  #model.out.file <- paste("/shared/2/projects/cross-lingual-exchange/models/regression_",outcome,".Rds",sep='')     
  #saveRDS(model,file=model.out.file)

  model.table.file <- paste('/shared/2/projects/cross-lingual-exchange/results/country_var_model_',outcome,'_',date,'.tsv',sep='')
  write.table(res, file = model.table.file, sep="\t", row.names=FALSE)
  models[[i]] <- model
}
res <- stargazer(models,single.row=TRUE,report = "vc*", type='text',
          header = FALSE, 
          df=FALSE, 
          digits=3, 
          se = NULL,
          align=TRUE,
          out=paste('/shared/2/projects/cross-lingual-exchange/results/country_var_effects_',date,'.tex',sep=''))
