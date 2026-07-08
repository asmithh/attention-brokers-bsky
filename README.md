# Attention Brokers on Bluesky
This repo contains code and some data to analyze follower accumulation patterns before and after an attention broker's reposts. Attention brokers, or *tertius amplificans,* are influential accounts whose amplification (i.e. reposting) of other accounts increases the rate at which their followers follow the amplified accounts.  

## Python Files
* `get_control_and_reposted_full_trajectories.py`: This lets us collate a dataset for causal inference. We keep track of the total accumulated follows from an attention broker's followers and non-followers to reposted (followed by the attention broker & reposted in the time period of interest) and control accounts (followed by the attention broker but never reposted). We run this as `python3 get_control_and_reposted_full_trajectories HANDLES_IN.txt $HRS_PERIOD`, where `HANDLES_IN.txt` is a .txt file with an attention broker's Bluesky handle on each line. `HRS_PERIOD` is a float indicating how many hours each time period we count follows for should be. Right now I'm using 2 hours' worth of granularity; sub-hour reslution may be worth exploring.
* `get_reposts.py`: used to obtain reposts for a given Bluesky handle (or list of handles) and write the data to a JSON blob.
* `utils.py`: Contains utility functions for data parsing.

### Other Code Files
* `plots.ipynb` contains functionality for plotting overall trends in per-capita per-day follower accumulation rates for control and reposted accounts for followers and non-followers (a total of 4 curves are plotted). It can also rehydrate original posts to recover the creation time of the original content that an attention broker reposted & plot the distribution of time elapsed between post creation and reposting by the attention broker.
* `panel_did2s.R` conducts an event study using [did2s](https://github.com/kylebutts/did2s/tree/main). It plots the event study results, checks for robustness using [HonestDiD](https://asheshrambachan.r-universe.dev/HonestDiD), and has functionality to output regression results in raw LaTeX code. 

## Using the Repo
You will need to run `get_reposts.py` to get repost data; this is not included due to privacy concerns. You will also need to obtain `follows_all.csv`, which is the entire Bluesky follow graph with precise follow event timings; this is also not included due to privacy concerns. Once you have the required files (JSON blobs of repost data & the follow graph), you can run `get_control_and_reposted_full_trajectories.py`, which allows you to obtain follower accumulation data for reposted & control accounts. Once you have the follower accumulation data, you can first plot the mean following rates with 95% bootstrapped confidence intervals, then create time-to-repost distribution plots, using `plots.ipynb`. The CSV from `get_control_and_reposted_full_trajectories.py` is also used as input to `panel_did2s.R`, which conducts the DiD analysis plus robustness checks.

### Notes:
In order to run this pipeline from scratch for a new attention broker, you'll need to have the following:
* JSON blobs of an attention broker's reposts, with each entry containing keys `reposted`, with the ATProto URI of the reposted content; `uri`, with the ATProto URI of the *repost*; and `created_at`, a datetime string indicating when the content was *reposted*. This should live in the directory `bsky_reposts/` and have the filename `$HANDLE.json`, where `$HANDLE` is the reposter's Bluesky handle. `get_reposts.py` contains functionality for creating these files by querying the Bluesky API.
* A JSON dict mapping Bluesky handles of potential attention brokers to their DIDs (should be `./handles_to_dids.json`; not included here due to privacy concerns).
* All non-deleted timestamped following events; this is referred to as `follows_all.csv` in this repo. It contains columns `from`, with the DID of the *follower*; `to`, with the DID of the followed account (i.e. followee); and `created-at`, indicating when the follow event occurred. Note that the version of `follows_all.csv` used in this project has multiple formats for datetimes. Around 0.5% of all datetimes could not be parsed using either of two formats, so we omit these follow events from the dataset. 
* About 400 GB of RAM to run the data extraction scripts and a non-trivial amount of compute time; depending on the number of reposts by an attention broker, extracting population counts could take over 20 hours. It takes at least half an hour just to load the follow network into `polars`, so we suggest amortizing compute time per attention broker by processing multiple attention brokers per run. This is why the collation code takes a file of Bluesky handles as input rather than a single handle. 

## Files That Aren't Code
* `plots/event_study_did2s` contains event study plots, which show the causal effect of the repost on following rates over time.
* `plots/timing` has plots that show the distribution of time elapsed between original post and repost for each attention broker.
* `plots/honestdid` contains plots indicating the relative magnitude of post-treatment parallel trends violations, as compared to pre-treatment violations, at which our results lose statistical significance.
* `total_follower_accumulation_data/*.csv.gz` contains gzipped CSVS with total follower accumulation data for each attention broker's followers and non-followers for reposted and control accounts. Note that these files can be upwards of 150 MB when expanded, so storing them without compression in Github will be difficult.
* `requirements.txt` contains minimal Python package requirements to use this code.



