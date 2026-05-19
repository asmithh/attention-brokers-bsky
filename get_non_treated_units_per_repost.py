import datetime as dt
import json
import os
import sys

import polars as pl
from polars.datatypes import String, Int64, Datetime

from config import FILEPATH, FILEPATH_OUT, AB_DIDS, REPOST_CUTOFF
from utils import *
"""
Makes CSVs with the following columns:
    gain_rate: int; how many accounts of type ever_treated followed unit_id on day time_period?
    ever_treated: bool; indicates if the accounts that followed unit_id in this row were followers or non-followers of the attention broker
    unit_id: int; identifies the reposted account (i.e. the account whose content the attention broker reposted)
    time_period: int; number of days elapsed since earliest repost event in the dataset
    ts: int; days relative to the repost event

Some combinations of [ever-treated, unit_id, and ts] will be missing; we handle these with a separate script.

Run as python3 get_non_treated_units_per_repost.py inf.txt $DAYS_FWD $DAYS_BWD
inf.txt is a text file with one Bluesky handle of an attention broker per line
DAYS_FWD is the number of days for which we want data after the repost
DAYS_BWD is the number of days for which we want data before the repost
"""

AB_HANDLES = []
with open(sys.argv[1], 'r') as f:
    for line in f.readlines():
        AB_HANDLES.append(line.strip())
        
DAYS_FWD = int(sys.argv[2])
DAYS_BWD = int(sys.argv[3])

df_follows = load_df_follows(FILEPATH)


def make_control_csv(HANDLE, df_follows, days_fwd, days_bwd, n_controls=3):
    """
    Make CSV of data for use with difference-in-differences. Columns are explained at the top of the file.

    Inputs:
        HANDLE: attention broker's Bluesky handle
        df_follows: polars dataframe of follow events w/ columns 
            from (follower), to (followed), and created_at (follow timestamp)

    Outputs:
        No output variables; writes output to a .csv file
    """
    ab_did = AB_DIDS[handle] # get DID of attention broker

    df_reposts, min_repost_day, tot_reposts = make_repost_df(FILEPATH, handle, ab_did)

    followers_of_ab, followed_by_ab, accts_to_unit_id = get_followed_accts_and_unit_ids(
        df_follows, 
        handle, 
        ab_did, 
        df_reposts
    )

    data_final = [] # will be used to create dataframe of DiD data.

    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        # this looks weird, but it means I can do polars dataframe math with repost_created_at
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
        repost_period = (repost_created_at.item() - min_repost_day).days # relative date of repost
        
        orig_poster = row['orig_poster'] # referred to as OP (original poster) in these comments
        low_follow_bound = row['created_at'] - dt.timedelta(days=days_bwd) # the minimum day we will collect following data for
        high_follow_bound = row['created_at'] + dt.timedelta(days=days_fwd) # the maximum day we will collect following data for

        ops_reposted_before_this_repost = df_reposts.filter(
            pl.col('created_at') <= high_follow_bound
        )
        followed_to_sample_from = followed_by_ab.join(
            ops_reposted_before_this_repost,
            left_on='to',
            right_on='orig_poster',
            how='anti',
        )
        followed_sample = followed_to_sample_from.sample(n=n_controls)

        for en, sample in enumerate(followed_sample.iter_rows(named=True)):
            follows_to_control_following_ab = get_follows_to_reposted_account(
                df_follows,
                sample['to'],
                followers_of_ab, 
                repost_created_at,
                high_follow_bound,
                low_follow_bound,
            )

            followers_before_repost, followers_after_repost = partition_follows_before_after_repost(
                follows_to_control_following_ab,
                repost_created_at,
            )

            followers_before_repost = determine_attention_broker_followers(followers_before_repost, before=True)
            followers_after_repost = determine_attention_broker_followers(followers_after_repost, before=False)

            if sample['to'] not in control_unit_ids:
                control_unit_ids[sample['to']] = max_unit_id
                max_unit_id += 1
            
            curr_unit_id = accts_to_unit_id[sample['to']]

            data_final = extend_final_dataframe(
                data_final, 
                followers_before_repost, 
                followers_after_repost, 
                curr_unit_id, 
                repost_period
            )
            
    # build dataframe from list of dicts
    data = pl.DataFrame(data_final)    
    fwd_str = str(DAYS_FWD)
    bwd_str = str(DAYS_BWD)
    data.write_csv(f'{FILEPATH_OUT}/control_csvs/{handle}_fwd_{DAYS_FWD}_bwd_{DAYS_BWD}.csv')
    print('done')

for AB_HANDLE in AB_HANDLES:
    make_control_csv(AB_HANDLE, df_follows, DAYS_FWD, DAYS_BWD)
# DID_FILES = os.listdir(f'{FILEPATH}/did_csvs')
# PROCESSED_HANDLES = set([d[:-4] for d in DID_FILES])
# for handle in list(AB_DIDS.keys()):
#     print(handle)
#     if handle not in PROCESSED_HANDLES:
#         make_did_csv(handle, df_follows)
