library(margins)
library(MatchIt)


#topics_of_interest <- c(1,10,19,24,44,3,16,31,11,23,30,41,46,47,48)
outcomes <- c('domains','temporal_hashtags','betweenness')
date <- '11-02-2021'
#topic.files <- rep(NA,length(topics_of_interest))
overall.files <- rep(NA,length(outcomes))
# for(i in 1:length(topics_of_interest)){
#     topic.files[i] <- paste('overall','topic',i,date,sep='_')
# }
for(i in 1:length(outcomes)){
    overall.files[i] <- paste('all',outcomes[i],date,sep='_')
}

#all.files <- c(topic.files,overall.files)

model.dir <- '/shared/2/projects/cross-lingual-exchange/models/causal_inference/'

for(file.name in overall.files){
    print(file.name)
    regression.file <- paste(model.dir,file.name,'_regress.Rds',sep='')
    matchit.file <- paste(model.dir,file.name,'_matchit.Rds',sep='')
    out.file <- paste(model.dir,'margins_',file.name,'.tsv',sep='')
    m <- readRDS(matchit.file)
    m.data <- match.data(m)
    model <- readRDS(regression.file)
    marg <- margins(model)
    print(summary(marg))
    write.table(summary(marg), file = out.file, sep="\t", row.names=FALSE)
}
