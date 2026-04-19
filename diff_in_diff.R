library(fixest)
library(HonestDiD)
library(data.table)
library(dplyr)    # alternatively, this also loads %>%
library(ggplot2)
library(glue)
library(dotwhisker)
library(ggfixest)
library(kableExtra)
library(modelsummary)
library(jsonlite)
library(broom)

cls = c(
  period = "numeric",
  post.treat = "factor",
  unit_id = "factor",
  gain_rate_fol = "numeric",
  gain_rate_non = "numeric",
  ts = "numeric",
  year_treated = "numeric"
)

acct = 'swiftonsecurity.com'
json_data <- read_json(glue(
  "~/attention-brokers-bsky/population_counts/{acct}_fwd_14_bwd_14.json"))
pop_fol = json_data$ab_followers
pop_non = json_data$non_followers
fpath_did = '~/attention-brokers-bsky/processed_did_csvs'
fname_did = glue('{fpath_did}/{acct}_processed_did_data.csv')
data_did = fread(
  fname_did,
  colClasses=cls
)
data_did$treat = 1

fpath_con = '~/attention-brokers-bsky/processed_control_csvs'
fname_con = glue('{fpath_con}/{acct}_processed_did_data.csv')
data_con = fread(
  fname_con,
  colClasses=cls
)
data_con$treat = 0

dat_list = list(data_did, data_con)
data = rbindlist(dat_list)

# try fepois
twfe_fol = fepois(gain_rate_fol ~ i(ts, treat, ref=-14)  | 
               unit_id + period, cluster=~unit_id, data=data)

twfe_non = fepois(gain_rate_non ~ i(ts, treat, ref=-14)   | 
               unit_id + period, cluster=~unit_id, data=data)

ggiplot(
  list("Followers"=twfe_fol, "Non-Followers"=twfe_non), 
  main=glue("{acct}: \n Effect of Repost on Follow Rate"), 
  col=c("red", "steelblue"),
  xlab="Time Relative to Repost",
  multi_style="facet",
  facet_args = list(ncol = 2)
) +
  theme(plot.title = element_text(hjust = 0.5))

# betahat <- summary(twfe_fol)$coefficients #save the coefficients
# sigma <- summary(twfe_fol)$cov.scaled
# 
# delta_rm_results <-
#   HonestDiD::createSensitivityResults_relativeMagnitudes(
#     betahat = betahat, #coefficients
#     sigma = sigma, #covariance matrix
#     numPrePeriods = 13, #num. of pre-treatment coefs
#     numPostPeriods = 13, #num. of post-treatment coefs
#     Mbarvec = seq(0.5,2,by=0.5) #values of Mbar
# )

get_coefs <- function(twfe, ix) {
  orig_estimate <- unlist(twfe$coefficients[ix])
  orig_se <- unlist(twfe$se[ix])
  return(c(as.numeric(orig_estimate), as.numeric(orig_se)))
}

compare_coefs <- function(twfe0, twfe1, ix0, ix1){
  coefs0 <- get_coefs(twfe0, ix0)
  estimate0 <- coefs0[1]
  se0 <- coefs0[2]

  coefs1 <- get_coefs(twfe1, ix1)
  estimate1 <- coefs1[1]
  se1 <- coefs1[2]

  return((estimate1 - estimate0) / (sqrt(se0^2 + se1 ^ 2)))

}
# 
# compare_coefs(twfe_fol, twfe_non, 13)
# pnorm(compare_coefs(twfe_fol, twfe_non, 13))
# compare_coefs(simple_fol, simple_non, 1)
# pnorm(compare_coefs(simple_fol, simple_non, 1))


# msummary(
#   simple_fol,
#   signif.stars = TRUE,
#   fmt = fmt_significant(3),
#   shape=term ~ model + statistic,
#   statistic = c( "statistic", "std.error", "p.value", "conf.low", "conf.high"),
#   output="latex")

write.csv(tidy(twfe_fol), glue('~/attention-brokers-bsky/r_out/{acct}_twfe_fol.csv'))

write.csv(tidy(twfe_non), glue('~/attention-brokers-bsky/r_out/{acct}_twfe_non.csv'))
