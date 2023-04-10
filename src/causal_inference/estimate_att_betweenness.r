library(lme4)
library(lmerTest)
library(stargazer)
library(MatchIt)
library("lmtest") #coeftest
library("sandwich") #vcovCL

date <- '11-02-2021'
country.pair.dirs <- list.dirs(path="/shared/2/projects/cross-lingual-exchange/data/betweenness_dataframes")
country.pair.dirs <- country.pair.dirs[2:length(country.pair.dirs)]
out.file <- paste("/shared/2/projects/cross-lingual-exchange/results/att_betweenness_",date,".tsv",sep='')

x <- data.frame('Country Pair', 'Country', 'Estimate', 'Robust Std. Error', 'Robust P-value',
'Treated','Untreated')
write.table(x, file = out.file, sep = "\t", append = TRUE, quote = FALSE,
            col.names = FALSE, row.names = FALSE)

all.data.frames <- vector(mode="list",length=2*length(country.pair.dirs))

for(i in 1:length(country.pair.dirs)){
    country.pair.dir <- country.pair.dirs[[i]]
    country.pair <- unlist(strsplit(basename(country.pair.dir),'_'))
    for(j in 1:2){
        country <- country.pair[[j]]
        out.file.model.base <- paste("/shared/2/projects/cross-lingual-exchange/models/causal_inference",paste(country.pair[[1]],country.pair[[2]],country,'betweenness',date,sep='_'),sep='/')
        filename <- paste(country.pair.dir,"/",country,".tsv",sep='')     
        df <- read.table(filename,header=TRUE,sep='\t')
        ix <- as.integer(2*i + 1 - j)
        df <- df[df$monolingual_minority == 0,]  #units must post in majority language (either monolingual or bilingual)
        df$id_str <- NULL
        df$country <- NULL 
        df <- df[!rowSums(df < 0), ]
        df$log_degree <- log(df$num_neighbors + 1)
        df$log_friends <- log(df$friends_count + 1)
        df$log_followers <- log(df$followers_count + 1)
        df$log_statuses <- log(df$statuses_count + 1)
        df$log_tweets <- log(df$decahose_tweet_count + 1)
        df$log_favourites <- log(df$favourites_count + 1)
        df$scaled_betw <- 1000000 * df$betw
        df$log_betw <- log(df$scaled_betw + 1)
        df$log_rate <- log(df$post_rate + 1)
        df$log_age <- log(df$account_age + 1) # make that linear
        all.data.frames[[ix]] <- df
        treated <- nrow(df[(df$is_bilingual == 1),])
        untreated<- nrow(df[(df$is_bilingual == 0),])

        if (sum(df$is_bilingual) >= 25){
            m <- matchit(is_bilingual ~ log_degree 
                     + verified + log_friends + log_followers + log_statuses + log_favourites
                     + log_age + log_rate + log_tweets,
                     data=df,method='subclass',subclass=25,estimand='ATT')
            saveRDS(m,file=paste(out.file.model.base,'matchit.Rds',sep='_'))
            saveRDS(summary(m)$sum.across,file=paste(out.file.model.base,'balance.Rds',sep='_'))

            m.data <- match.data(m)

            strata.out.file <- paste(out.file.model.base,'strata.tsv',sep='_')
            write.table(data.frame('Stratum','Treated','Control', 'Total'), file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)

            for(stratum in 1:25){
                m.data.sub <- m.data[m.data$subclass == stratum,]
                m.data.sub.total <- nrow(m.data.sub)
                m.data.sub.treated <- sum(m.data.sub$is_bilingual)
                m.data.sub.control <- m.data.sub.total - m.data.sub.treated
                s <- c(stratum, m.data.sub.treated,m.data.sub.control,m.data.sub.total)
                s <- as.matrix(t(s))
                write.table(s, file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
            }

            fit <- lm(log_betw ~ is_bilingual + log_degree 
                     + verified + log_friends + log_followers + log_statuses + log_favourites
                     + log_age + log_rate + log_tweets
                     ,data=m.data,weights=weights)
            saveRDS(fit,file=paste(out.file.model.base,'regress.Rds',sep='_'))

            res <- coeftest(fit,vcov.=vcovHC)
            x <- c(paste(country.pair[1],country.pair[2],sep='_'),country,res[2,1],res[2,2],res[2,4],treated,untreated)
            cat(i,j,x,'\n')
            x <- as.matrix(t(x))
            write.table(x, file = out.file, sep = "\t", append = TRUE, quote = FALSE,
                        col.names = FALSE, row.names = FALSE)
        }
    }
}

df <- do.call("rbind", all.data.frames)
out.file.model.base <- paste("/shared/2/projects/cross-lingual-exchange/models/",paste('all','betweenness',date,sep='_'),sep='')
treated <- nrow(df[(df$is_bilingual == 1),])
untreated <- nrow(df[(df$is_bilingual == 0),])


m <- matchit(is_bilingual ~ log_degree
           + verified + log_friends + log_followers + log_statuses + log_favourites
           + log_age + log_rate + log_tweets,
           data=df,method='subclass',subclass=25,
           estimand='ATT')
m.data <- match.data(m)
saveRDS(m,file=paste(out.file.model.base,'matchit.Rds',sep='_'))
saveRDS(summary(m)$sum.across,file=paste(out.file.model.base,'balance.Rds',sep='_'))


strata.out.file <- paste(out.file.model.base,'strata.tsv',sep='_')
write.table(data.frame('Stratum','Treated','Control', 'Total'), file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
for(stratum in 1:25){
    m.data.sub <- m.data[m.data$subclass == stratum,]
    m.data.sub.total <- nrow(m.data.sub)
    m.data.sub.treated <- sum(m.data.sub$is_bilingual)
    m.data.sub.control <- m.data.sub.total - m.data.sub.treated
    s <- c(stratum, m.data.sub.treated,m.data.sub.control,m.data.sub.total)
    s <- as.matrix(t(s))
    write.table(s, file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
}

fit <- lm(log_betw ~ is_bilingual + log_degree
           + verified + log_friends + log_followers + log_statuses + log_favourites
           + log_age + log_rate + log_tweets
           ,data=m.data,weights=weights)
saveRDS(fit,file=paste(out.file.model.base,'regress.Rds',sep='_'))

res <- coeftest(fit,vcov.=vcovHC)
est <- res[2,1]
se.robust <- res[2,2]
p.robust <- res[2,4]
x <- c('all','all',est,se.robust,p.robust,treated,untreated)
cat(x,'\n')
x <- as.matrix(t(x))
write.table(x, file = out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
