#!/usr/bin/env Rscript
library(data.table)
library(glue)
library(HonestDiD)
library(dplyr) 
library(did2s)
library(ggfixest)
library(ggplot2)
library(jsonlite)

cls = c(
  unit_id = "numeric",
  period = "numeric",
  ever_treated = "factor",
  period_treated = "numeric",
  tot_ab_fol = "numeric",
  tot_non_fol = "numeric"
)

args = commandArgs(trailingOnly=TRUE)

periods_out = 12
fpath_did = '/home/nte5cp/attention-brokers-bsky'

acct_file = glue('{fpath_did}/candidates_with_dids_by_20250915.csv')
acct_dat = fread(acct_file)
accts = acct_dat$id
acct = accts[strtoi(args[1]) + 1]
print("acct number")
print(acct)
fname_did = glue('{fpath_did}/panel_data/{acct}_follows_to_ops_and_never_reposted_controls_period_2.0_hrs.csv')

data = fread(
  fname_did,
  colClasses=cls
)

print("number of controls")
controls = data %>% filter(data$ever_treated == 0)
print(length(unique(controls$unit_id)))
if (length(unique(controls$unit_id)) > 5000) {
  print("scaling controls down to 5000")
    controls %>% 
  group_by("unit_id") %>%
  sample_n(size = 5000, replace = FALSE) %>%
  ungroup()
} 

print("number of reposts")
reposts = data %>% filter(data$ever_treated == 1)
print(length(unique(reposts$unit_id)))

if (length(unique(reposts$unit_id)) > 5000) {
  print("scaling reposts down to 5000")
    reposts %>% 
  group_by("unit_id") %>%
  sample_n(size = 5000, replace = FALSE) %>%
  ungroup()
} 

data <- rbind(reposts, controls)
data$rel_period <- if_else(
  data$period_treated == 10000,
  Inf,
  data$period - data$period_treated,
)

data$tot_ab_fol = data$tot_ab_fol # / pops$ab_fol
data$tot_non_fol = data$tot_non_fol # / pops$non_fol
data$log_tot_ab_fol = log(data$tot_ab_fol)
data$log_tot_non_fol = log(data$tot_non_fol)
data$treat <- (data$period >= data$period_treated) * 1
data$filter_out <- if_else(
  (data$period_treated == 10000 | abs(data$rel_period) < periods_out + 1),
  0,
  1
)
data <- data %>% 
  filter(! filter_out)

es_fol <- did2s(data,
                yname = "log_tot_ab_fol", 
                first_stage = ~ 0 | unit_id + period, 
                second_stage = ~i(rel_period, ref=c(Inf)), 
                treatment = "treat", 
                cluster_var = "unit_id",
)

es_non <- did2s(data,
                yname = "log_tot_non_fol", 
                first_stage = ~ 0 | unit_id + period, 
                second_stage = ~i(rel_period, ref=c(Inf)), 
                treatment = "treat", 
                cluster_var = "unit_id",
)

ggiplot(
  list("Followers" = es_fol, "Non-Followers" = es_non),
  drop="Inf",
  col = c("red", "steelblue"),
  main = glue("{acct}:\nEvent study w/ Staggered Treatment"),
  xlab = glue("2-hour periods to repost"),
  multi_style="facet",
  ref.line = 0.0,
  facet_args = list(ncol = 2)
)

# Uncomment to do sensitivity plots; not recommended for batch computations.
# sensitivity_results <- es_fol |>
#   # Take fixest obj and convert for `honest_did_did2s`
#   get_honestdid_obj_did2s(coef_name = "rel_period") |>
#   # Run sensitivity analysis
#   honest_did_did2s(
#     e = 1,
#     type = "relative_magnitude",
#     Mbarvec = seq(from = 0.5, to = 4, by = 0.5)
#   )
# HonestDiD::createSensitivityPlot_relativeMagnitudes(
#   sensitivity_results$robust_ci,
#   sensitivity_results$orig_ci
# ) +
#   ggtitle(glue("Sensitivity Analysis on Relative Magnitude \n for {acct}")) +
#   theme(
#     plot.title=element_text( hjust=0.5, face='bold')
#   )

compare_coefs <- function(estimate0, se0, estimate1, se1) {
  zstat = (estimate1 - estimate0) / (sqrt(se0^2 + se1 ^ 2))
  print(zstat)
  return(pnorm(zstat))
}

table_fol <- broom::tidy(es_fol)
table_non <- broom::tidy(es_non)
print("testing if followers' coeff at 0 significantly less than coeff at 1")
compare_coefs(
  table_fol$estimate[periods_out + 2],
  table_fol$std.error[periods_out + 2],
  table_fol$estimate[periods_out + 1],
  table_fol$std.error[periods_out + 1]
)
print("testing if non-followers' coeff at 1 significantly less than followers' coeff at 1")

compare_coefs(
  table_fol$estimate[periods_out + 2],
  table_fol$std.error[periods_out + 2],
  table_non$estimate[periods_out + 2],
  table_non$std.error[periods_out + 2]
)

print("testing if followers' coeff at -1 significantly less than coeff at 0")
compare_coefs(
  table_fol$estimate[periods_out + 1],
  table_fol$std.error[periods_out + 1],
  table_fol$estimate[periods_out],
  table_fol$std.error[periods_out]
)


print("Is effect size at t = 1 significant?")
print(table_fol$estimate[periods_out + 2])
print(table_fol$std.error[periods_out + 2])