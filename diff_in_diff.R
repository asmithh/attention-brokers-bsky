library(fixest)
library(HonestDiD)
library(data.table)
library(dplyr)    # alternatively, this also loads %>%
library(ggplot2)

cls = c(
  period = "numeric",
  post.treat = "factor",
  unit_id = "factor",
  gain_rate_fol = "numeric",
  gain_rate_non = "numeric",
  ts = "numeric"
)

data = fread(
  '~/attention-brokers-bsky/processed_did_csvs/jamellebouie.net_processed_did_data.csv', 
  colClasses=cls
)

simple_fol = feols(gain_rate_fol ~ post.treat | unit_id + period, data=data)
simple_non = feols(gain_rate_non ~ post.treat | unit_id + period, data=data)


twfe_fol = feols(gain_rate_fol ~ i(ts, ref=-13)  | 
               unit_id, cluster=~unit_id, data=data)

twfe_non = feols(gain_rate_non ~ i(ts, ref=-13)  | 
               unit_id, cluster=~unit_id, data=data)
iplot(
  list(twfe_fol, twfe_non), 
  main="jamellebouie.net: Effect of Retweet on Follow Rate", 
  col=c("red", "steelblue")
)
