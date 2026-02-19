# Attention Brokers on Bluesky
This repo contains code to analyze follow patterns before and after an attention broker's reposts. Attention brokers, or *tertius amplificans,* are influential accounts whose amplification (i.e. reposting) of other accounts increases the rate at which their followers follow the amplified accounts.  

## Python Files
* `count_follower_non_follower_populations.py`: To obtain per-capita per-day following rates for each account $R$ reposted by an attention broker $A$, we need to figure out the denominator in the per-capita equation. In other words, how many of $A$'s followers are actively following other accounts, and how many accounts that don't follow $A$ are actively following the kinds of accounts that $A$ reposts? Right now we're estimating this as the number of unique accounts that followed *any* account reposted by the attention broker. We run this as `python3 count_follower_non_follower_populations.py HANDLES_IN.txt $DAYS_FWD $DAYS_BWD`, where `HANDLES_IN.txt` is a .txt file with an attention broker's Bluesky handle on each line. `DAYS_FWD` refers to the number of days after the repost for which we're collecting data, and `DAYS_BWD` refers to the number of days before the repost for which we're collecting data. Right now I'm using 14 for both.
* `interpolate_missing_did_data.py`: `parse_reposts_and_extract_follow_timings.py` does not have rows for combinations of {day, reposted account, follower/non-follower} where no follows occurred. In order to run the actual differences-in-differences analysis or visualize the per-day per-capita follow rates, we need to add in those missing rows and assign a follower count of $0$. This is what this script does. We run it as `python3 interpolate_missing_did_data.py $HANDLE $DAYS_FWD $DAYS_BWD`. `HANDLE` is the attention broker's Bluesky handle; `DAYS_FWD` and `DAYS_BWD` refer to the number of days of data we collect after & before the repost, respectively.
* `parse_reposts_and_extract_follow_timings.py`: Makes CSVs with the following columns:
    * `gain_rate`: number of followers accumulated by the reposted account
    * `ever_treated`: Boolean indicating whether the followers accumulated are followers of the attention broker or not
    * `unit_id`: Identifies the reposted account
    * `time_period`: Number of days elapsed since the earliest repost in the dataset (keeps track of absolute time)
    * `ts`: Days relative to the repost event (ranges from -1 * DAYS_BWD to DAYS_FWD).
Note that rows where `gain_rate` = 0 will be missing from the resulting CSV. We run this script as `python3 parse_reposts_and_extract_follow_timings.py HANDLES_IN.txt $DAYS_FWD $DAYS_BWD`, where `HANDLES_IN.txt` is a .txt file with an attention broker's Bluesky handle on each line. `DAYS_FWD` refers to the number of days after the repost for which we're collecting data, and `DAYS_BWD` refers to the number of days before the repost for which we're collecting data. Right now I'm using 14 for both.
* `utils.py`: Contains utility functions for data parsing.

### Other Code Files
* `plots.ipynb` contains functionality for plotting overall trends in per-capita per-day follow rates for followers and non-followers. It also produces a dataframe for use in `diff_in_diff.R`. 
* `diff_in_diff.R` conducts a simple DiD analysis as well as an event study and compares coefficients for followers and non-followers. It plots the DiD analysis results and the event study results, and it has functionality to output regression results in raw LaTeX code.

### Deprecated Code
* `deprecated_code/make_mark_capture_histories.py`: Was used to create capture histories for MARK, which is a program that does population estimation given a text file where each line is a string of 0s and 1s. A value of 1 at string index $k$ on line $i$ indicates that individual $i$ was "captured" (i.e. seen following at least one account) on day $k$; 0 indicates we didn't see individual $i$ following anyone on day $k$. I'm not currently using MARK to estimate population because I've been running into numeric convergence issues with larger populations.

## Using the Repo
We need to run `parse_reposts_and_extract_follow_timings.py` and `count_follower_non_follower_populations.py` so that each attention broker account is covered. Once `parse_reposts_and_extract_follow_timings.py` has produced a CSV, we can run `interpolate_missing_did_data.py` to produce interpolated raw follow counts. Once we have both interpolated follower counts and follower/non-follower populations, we can first plot the mean following rates with 95% bootstrapped confidence intervals, then create the actual CSV for differences-in-differences, using `plots.ipynb`. The final CSV is input to `diff_in_diff.R`, which conducts a simple DiD analysis as well as an event study and compares coefficients for followers and non-followers. 

### Running from Scratch
In order to run this pipeline from scratch for a new attention broker, you'll need to have the following:
* JSON blobs of an attention broker's reposts, with each entry containing keys `reposted`, with the ATProto URI of the reposted content; `uri`, with the ATProto URI of the *repost*; and `created_at`, a datetime string indicating when the content was *reposted*. This should live in the directory `bsky_reposts/` and have the filename `$HANDLE.json`, where `$HANDLE` is the reposter's Bluesky handle.
* All non-deleted timestamped following events; this is referred to as `follows_all.csv` in this repo. It contains columns `from`, with the DID of the *follower*; `to`, with the DID of the followed account (i.e. followee); and `created-at`, indicating when the follow event occurred. Note that the version of `follows_all.csv` used in this project has multiple formats for datetimes. As noted in `changelog.txt`, around 0.5% of all datetimes could not be parsed using either of two formats, so we omit these follow events from the dataset. 
* About 400 GB of RAM to run the data extraction scripts and a non-trivial amount of compute time; depending on the number of reposts by an attention broker, extracting population counts could take over 20 hours. It takes a couple hours just to load the follow network into `polars`, so we suggest amortizing compute time per attention broker by processing multiple attention brokers per run. This is why the extraction code takes a file of Bluesky handles as input rather than a single handle. 

## Files That Aren't Code
* `plots/event_studies` contains event study plots, which show the causal effect of the repost on following rates over time.
* `plots/trends` has plots with mean following rates and 95% bootstrapped confidence intervals; since we don't have population numbers for each attention broker yet, some of these plots show raw follow counts. 
* `plots/simple_did` contains plots comparing the effect of "treatment" (i.e. exposure to an attention broker's repost) in a simple differences-in-differences analysis for followers and non-followers.  
* `changelog.txt` details the changes I've made to the code and the problems I've run into & corrected.
* `population_counts/*.json` contains population count JSON files for each attention broker we studied in this analysis.
* `interpolated_did_csvs/*.csv` contains the output of `interpolate_missing_did_data.py` for each attention broker we studied in this analysis. 


