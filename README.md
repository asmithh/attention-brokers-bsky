# Attention Brokers on Bluesky
This repo contains replication code to analyze follower accumulation patterns before and after a potential attention broker's reposts. Attention brokers, or *tertius amplificans,* are influential accounts whose amplification (i.e. reposting) of other accounts increases the rate at which their followers follow the amplified accounts. 

The analyses in this repository rely on a non-anonymized version of the "A Blue Start" [dataset](https://www.nature.com/articles/s41597-026-06920-1.pdf) by Smith et al. (2026); due to privacy concerns, we cannot include the raw data products we used, but we can include anonymized intermediate data products. Some of these data products are too large to upload to GitHub or are still potentially sensitive, so we have created a restricted access [Zenodo repository](https://doi.org/10.5281/zenodo.23216334) to store these data products.

## Python Files
`config.py`:

contains configuration variables, mostly for file paths and temporal cutoffs. Please modify the variables in here to fit your computer's setup.

`get_control_and_reposted_full_trajectories.py`: 

This lets us collate a dataset for causal inference. We keep track of the total accumulated follows from an attention broker's followers and non-followers to reposted (followed by the attention broker & reposted in the time period of interest) and control accounts (followed by the attention broker but never reposted). We run this as 

`python3 get_control_and_reposted_full_trajectories $ACCT_RANK $HRS_PERIOD $TESTING $REWRITE_FILES`, 

where `ACCT_RANK` refers to the candidate account's rank order in terms of reach score; this is often used in slurm batch jobs to keep track of each account's processes and output files. `HRS_PERIOD` is a float indicating how many hours each time period we count follows for should be. Right now I'm using 2 hours' worth of granularity; sub-hour resolution may be worth exploring. `TESTING` indicates whether we should use a smaller subsample of the follower network (to make sure the code works as intended without using hundreds of gigabytes of RAM), and `REWRITE_FILES` indicates whether we want to overwrite existing output files for a given candidate account.

`get_reposts_bulk.py`: 

Used to obtain reposts for all candidate accounts (that could be attention brokers) and write the data for each candidate account to a JSON blob. Note that the file used to enumerate accounts, `candidates_with_dids_by_20250915.csv`, is not included in this repository or the accompanying replication data files; it links candidate accounts' Bluesky DIDs (distributed identifiers) to their anonymous integer IDs from the "A Blue Start" dataset, and publishing it publicly would destroy the anonymity of that dataset.

`utils.py`: 

Contains utility functions for data parsing and collation.

### Jupyter Notebooks
`descriptive_candidate_stats.ipynb`:

Contains code for computing descriptive statistics about candidate accounts' follower counts, reposting behavior, and reach scores (repost count times follower count). Used to understand how attention broker and non-attention broker accounts differ.

`parse_did2s_out.ipynb`:

Parses the output of `panel_did2s.R` (the results of the analysis wherein attention broker status is determined) and plots aggregate results. Compares account statuses, criterion failure counts, and effect sizes.

`repost_latency.ipynb`:

Analyzes the temporal dynamics of candidate accounts' online presences. Specifically, do attention brokers repost content more quickly after it is created, do they tend to have smaller intervals between posts, and do they spend smaller chunks of time offline compared to non-attention brokers?

## Other Files
### Data File
`candidate_accounts_anon_raw_by_20250915.csv` is an anonymized dataframe of candidate accounts that contains the following columns:

* `src-poster`: anonymous integer ID from the Smith et al. "A Blue Start" dataset
* `tgt_post`: number of reposts by the account before September 15, 2025
* `count(src_user)`: account's follower count
* `reposts_x_followers`: product of the account's follower count and repost count ("reach score")
  
### Other Code
* `panel_did2s.R` conducts an event study using [did2s](https://github.com/kylebutts/did2s/tree/main). It plots the event study results, can check for robustness using [HonestDiD](https://asheshrambachan.r-universe.dev/HonestDiD), and determines whether all four attention brokerage criteria are met.

### Figures for Publication
Can be found in `new_figs/`. 

## Using the Repo
You will need to run `get_reposts.py` to get repost data; this is not included due to privacy concerns. You will also need to obtain `follows_all.csv`, which is the entire Bluesky follow graph with precise follow event timings; this is also not included due to privacy concerns. Once you have the required files (JSON blobs of repost data & the follow graph), you can run `get_control_and_reposted_full_trajectories.py`, which allows you to obtain follower accumulation data for reposted & control accounts. Once you have the follower accumulation data, you can determine accounts' attention broker status using `panel_did2s.R`. The CSV from `get_control_and_reposted_full_trajectories.py` is used as input to `panel_did2s.R`, which conducts the DiD analysis plus attention brokerage criteria and optional robustness checks.

Some anonymized data products are not included in this repository because they were somewhat sensitive and/or the individual files were too large to upload to GitHub. These files can be found in our [Zenodo repository](https://doi.org/10.5281/zenodo.23216334). Instructions for inflating these files can be found in the Zenodo repository's description and `README.md`. We include the contents of the Zenodo repository's `README.md` here for completeness' sake:

### Repost + Original Content Timing JSON Blobs
The JSON blobs `original_posts_by_acct_id.json` and `repost_intervals_by_acct_id.json` each have anonymized account IDs from the Smith et al. ["A Blue Start"](https://www.nature.com/articles/s41597-026-06920-1.pdf) dataset. `original_posts_by_acct_id.json` maps account IDs to lists of timestamps at which the account produced either an original post or a reply to someone else's post. `repost_intervals_by_acct_id.json` maps each account ID to a dictionary with keys `repost-created-at`, indicating when the repost occurred, `op-posted-at`, indicating when the original content was created, and `hours_between_op_and_repost`, indicating how many hours elapsed between the original content creation and the repost.

Code to work with these files exists in the accompanying [GitHub repository](https://github.com/asmithh/attention-brokers-bsky), specifically in `repost_latency.ipynb`. 

### Two-Stage Differences-in-Differences Output
To expand the directory of two-stage differences-in-differences analyses' output, use `tar -xzvf did2s_out.tar.gz`. This will produce a directory of text files with names formatted as `outf_$RANK.txt`, where `RANK` refers to the rank order of a candidate account in terms of their reach score (repost count times follower count). `RANK` is in the range [0, 100). Each file contains the output of an R script that runs the two-stage differences-in-differences analysis and applies the attention brokerage criteria to the results of that analysis.

Code to parse these files exists in the accompanying [GitHub repository](https://github.com/asmithh/attention-brokers-bsky), specifically in `parse_did2s_out.ipynb`. 

### Collated Per-Period Follower Accumulation Data
To expand the collated follower accumulation data, follow these steps:

1. Use `cat panel_data_part_a* > panel_data.tar.gz` to join the partial panel data chunks into one `.tar.gz` archive.
2. Use `tar -xzvf panel_data.tar.gz` to expand the archive into a directory called `panel_data`. 

#### Contents
`panel_data` contains files whose names are formatted as `$ACCT_ID_follows_to_ops_and_never_reposted_controls_period_2.0_hrs.csv`, where `ACCT_ID` refers to an anonymous integer identifier used in the Smith et al. ["A Blue Start"](https://www.nature.com/articles/s41597-026-06920-1.pdf) dataset (e.g. `295_follows_to_ops_and_never_reposted_controls_period_2.0_hrs.csv`). 

Each file (one per candidate account) has the following columns:
* `unit_id`: integer identifier for an individual reposted or control account
* `period`: number of two-hour periods elapsed since the earliest repost in the dataset for that candidate account
* `ever_treated`: Boolean indicating whether the account was reposted (if 0, it is a control account)
* `period_treated`: period in which the account was reposted by the candidate account; 10000 if never reposted.
* `tot_ab_fol`: follow events from followers of the candidate account that occurred in that time period
* `tot_non_fol`: follow events from non-followers of the candidate account that occurred in that time period
   
Files may be missing if a candidate account had no reposts of accounts it followed in the time period studied.

These files are used in `panel_did2s.R` for the final analyses that determine whether a candidate account is an attention broker. The script used to generate these files can be found in the GitHub repository in `get_control_and_reposted_full_trajectories.py`. 


## Notes:
In order to run this pipeline from scratch for a new attention broker, you'll need to have the following:
* JSON blobs of an attention broker's reposts, with each entry containing keys `reposted`, with the ATProto URI of the reposted content; `uri`, with the ATProto URI of the *repost*; and `created_at`, a datetime string indicating when the content was *reposted*. This should live in the directory `bsky_reposts/` and have the filename `$HANDLE.json`, where `$HANDLE` is the reposter's Bluesky handle. `get_reposts.py` contains functionality for creating these files by querying the Bluesky API.
* A JSON dict mapping Bluesky handles of potential attention brokers to their DIDs (should be `./handles_to_dids.json`; not included here due to privacy concerns).
* All non-deleted timestamped following events; this is referred to as `follows_all.csv` in this repo. It contains columns `from`, with the DID of the *follower*; `to`, with the DID of the followed account (i.e. followee); and `created-at`, indicating when the follow event occurred. Note that the version of `follows_all.csv` used in this project has multiple formats for datetimes. Around 0.5% of all datetimes could not be parsed using either of two formats, so we omit these follow events from the dataset. 
* >600 GB of RAM to run the data extraction scripts and a non-trivial amount of compute time; depending on the number of reposts by an attention broker, extracting population counts could take over 48 hours.




