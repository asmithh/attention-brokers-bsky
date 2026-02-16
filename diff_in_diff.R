library(fixest)
library(HonestDiD)
library(data.table)
library(dplyr)    # alternatively, this also loads %>%
library(ggplot2)
library(glue)
library(dotwhisker)
library(ggfixest)

cls = c(
  period = "numeric",
  post.treat = "factor",
  unit_id = "factor",
  gain_rate_fol = "numeric",
  gain_rate_non = "numeric",
  ts = "numeric"
)

acct = 'flavorflav.bsky.social'
fpath = '~/attention-brokers-bsky/processed_did_csvs'
fname = glue('{fpath}/{acct}_processed_did_data.csv')
data = fread(
  fname,
  colClasses=cls
)

simple_fol = feols(gain_rate_fol ~ post.treat | unit_id + period, data=data)
simple_non = feols(gain_rate_non ~ post.treat | unit_id + period, data=data)


twfe_fol = feols(gain_rate_fol ~ i(ts, ref=-13)  | 
               unit_id, cluster=~unit_id, data=data)

twfe_non = feols(gain_rate_non ~ i(ts, ref=-13)  | 
               unit_id, cluster=~unit_id, data=data)
ggiplot(
  list("Followers"=twfe_fol, "Non-Followers"=twfe_non), 
  main=glue("{acct}: \n Effect of Retweet on Follow Rate"), 
  col=c("red", "steelblue"),
  xlab="Time Relative to Repost"
) +
  theme(plot.title = element_text(hjust = 0.5))

get_coefs <- function(twfe, ix) {
  orig_estimate <- unlist(twfe$coefficients[ix])
  orig_se <- unlist(twfe$se[ix])
  return(c(as.numeric(orig_estimate), as.numeric(orig_se)))
}

compare_coefs <- function(twfe0, twfe1, ix){
  coefs0 <- get_coefs(twfe0, ix)
  estimate0 <- coefs0[1]
  se0 <- coefs0[2]
  
  coefs1 <- get_coefs(twfe1, ix)
  estimate1 <- coefs1[1]
  se1 <- coefs1[2]
  
  return((estimate1 - estimate0) / (sqrt(se0^2 + se1 ^ 2)))
  
}
pnorm(compare_coefs(twfe_fol, twfe_non, 14))
pnorm(compare_coefs(simple_fol, simple_non, 1))
dwplot(
  list("Followers" = simple_fol, "Non-Followers" = simple_non),
)  +
  scale_color_manual(
    values=c("Followers" = "red", "Non-Followers" = "steelblue"))  +
  ggtitle(glue("{acct}: \n DiD Comparison for Followers and Non-Followers")) +
  xlim(0, 8e-04) +
  theme(plot.title = element_text(hjust = 0.5)) +
  xlab("Effect Size") + 
  coord_flip()
