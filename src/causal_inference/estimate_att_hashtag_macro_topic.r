library(lme4)
library(lmerTest)
library(stargazer)
library(MatchIt)
library("lmtest") #coeftest
library("sandwich") #vcovCL


date <- '11-02-2021'
entertainment_topics <- c(1,10,19,24,44)
promotion_topics <- c(3,16,31)
politics_topics <- c(11,23,30,41,46,47)
sports_topics <- c(48)
topics_of_interest <- c(1,10,19,24,44,3,16,31,11,23,30,41,46,47,48)

topic_list <- list(entertainment=entertainment_topics,
                   promotion=promotion_topics,
                   politics=politics_topics,
                   sports=sports_topics)

country.pair.dirs <- list.dirs(path="/shared/2/projects/cross-lingual-exchange/data/multilingual_friend_effect_dataframes_top100")
country.pair.dirs <- country.pair.dirs[2:length(country.pair.dirs)]

for (topic_id in 1:length(topic_list)){
    broad_topic_name <- names(topic_list)[[topic_id]]
    narrow_topics <- topic_list[[topic_id]]
    out.file <- paste("/shared/2/projects/cross-lingual-exchange/results/",
                      broad_topic_name,
                      "_att_temporal_hashtag_top100_with_rt.tsv",sep='')
    x <- data.frame('Country Pair','Country','Estimate', 'Std. Error', 'P-value', 'Robust Std. Error', 'Robust P-value',
        'Treated_Outcome0','Treated_Outcome1','Untreated_Outcome0','Untreated_Outcome1')    
    write.table(x, file = out.file, sep = "\t", append = TRUE, quote = FALSE, col.names = FALSE, row.names = FALSE)
    for(i in 1:length(country.pair.dirs)){
        country.pair.dir <- country.pair.dirs[[i]]
        country.pair <- unlist(strsplit(basename(country.pair.dir),'_'))
        for(j in 1:2){
            country <- country.pair[[j]]
            out.file.model.base <- paste("/shared/2/projects/cross-lingual-exchange/models/causal_inference",paste(country.pair[[1]],country.pair[[2]],country,broad_topic_name,date,sep='_'),sep='/')

            filename <- paste(country.pair.dir,"/",country.pair[[j]],".gz",sep='')
            df <- read.table(gzfile(filename),header=TRUE,sep='\t')
            ix <- as.integer(2*i + 1 - j)
            df <- df[df$monolingual_majority == 1,]
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
            df$outcome <- 0
            for(narrow_topic in narrow_topics){
              df$outcome <- df$outcome + 
                df[[paste("has_temporal_hashtags_top100_other_language_topic_",narrow_topic,sep='')]]
           }
            df$outcome <- as.numeric(df$outcome > 0)
            treated_outcome0 <- nrow(df[(df$outcome == 0) & (df$has_bilingual_neighbor == 1),])
            treated_outcome1 <- nrow(df[(df$outcome == 1) & (df$has_bilingual_neighbor == 1),])
            untreated_outcome0 <- nrow(df[(df$outcome == 0) & (df$has_bilingual_neighbor == 0),])
            untreated_outcome1 <- nrow(df[(df$outcome == 1) & (df$has_bilingual_neighbor == 0),])

            if (sum(df$has_bilingual_neighbor) >= 100 && sum(df$outcome)>=100){
                m <- matchit(has_bilingual_neighbor ~ log_degree
                           + verified + log_friends + log_followers + log_statuses + log_favourites
                           + log_age + log_rate + log_tweets,
                           data=df,method='subclass',subclass=25,
                           estimand='ATT')

                saveRDS(m,file=paste(out.file.model.base,'matchit.Rds',sep='_'))
                saveRDS(summary(m)$sum.across,file=paste(out.file.model.base,'balance.Rds',sep='_'))
            
                m.data <- match.data(m)
                strata.out.file <- paste(out.file.model.base,'strata.tsv',sep='_')
                write.table(data.frame('Stratum','Treated','Control', 'Total'), file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)

                for(stratum in 1:25){
                    m.data.sub <- m.data[m.data$subclass == stratum,]
                    m.data.sub.total <- nrow(m.data.sub)
                    m.data.sub.treated <- sum(m.data.sub$has_bilingual_neighbor)
                    m.data.sub.control <- m.data.sub.total - m.data.sub.treated
                    s <- c(stratum, m.data.sub.treated,m.data.sub.control,m.data.sub.total)
                    s <- as.matrix(t(s))
                    write.table(s, file = strata.out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
                }

                fit <- glm(outcome ~ has_bilingual_neighbor + log_degree
                           + verified + log_friends + log_followers + log_statuses + log_favourites
                           + log_age + log_rate + log_tweets
                           ,data=m.data,weights=weights,
                         family=quasibinomial(link="logit"),
                          )

                saveRDS(fit,file=paste(out.file.model.base,'regress.Rds',sep='_'))
                rr <- as.numeric(exp(coef(fit)[2]))
                se <- as.numeric(summary(fit)$coefficients[, 2][2])
                p <- as.numeric(summary(fit)$coefficients[,4][2])
                res.robust <- coeftest(fit,vcov. = vcovHC)
                se.robust <- res.robust[2,2]
                p.robust <- res.robust[2,4]
                x <- c(paste(country.pair[1],country.pair[2],sep='_'),country,rr,se,p,se.robust,p.robust,treated_outcome0,treated_outcome1,untreated_outcome0,untreated_outcome1)
                cat(i,j,x,'\n')
                x <- as.matrix(t(x))
                write.table(x, file = out.file, sep = "\t", append = TRUE, quote = FALSE,col.names = FALSE, row.names = FALSE)
            }
        }
    }     
}

