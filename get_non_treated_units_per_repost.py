import datetime as dt
import gc
import json
import os
import sys
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
from polars.datatypes import String, Int64, Datetime

from utils import *
"""
Makes CSVs with the following columns:
    gain_rate: int; how many accounts of type ever_treated followed unit_id on day time_period?
    ever_treated: bool; indicates if the accounts that followed unit_id in this row were followers or non-followers of the attention broker
    unit_id: int; identifies the reposted account (i.e. the account whose content the attention broker reposted)
    time_period: int; number of days elapsed since earliest repost event in the dataset
    ts: int; days relative to the repost event

Some combinations of [ever-treated, unit_id, and ts] will be missing; we handle these with a separate script.

Run as python3 parse_reposts_and_extract_follow_timings.py inf.txt $DAYS_FWD $DAYS_BWD
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

FILEPATH = '/scratch/nte5cp' # change this for your machine
AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))

FILEPATH_OUT = '/home/nte5cp'
# set conservative upper bound on repost events we'll study.
REPOST_CUTOFF = dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

# this takes a long time to load because it's 220 GB of data.
df_follows = pl.read_csv(
    f'{FILEPATH}/follows_all.csv', 
    has_header=False, 
    new_columns=["from", "to", "created_at"],
)
df_follows = df_follows.drop_nulls() # drop any empty values 
# parse datetimes; datetime formatting is somewhat inconsistent within the CSV.
# This will leave very few NaN datetimes that don't match either format (about 0.005 error rate).
df_follows = df_follows.with_columns(
    pl.when(
        pl.col('created_at').str.tail(1) == 'Z').then(
        pl.col('created_at').str.head(-1).str.to_datetime(
            format='%Y-%m-%dT%H:%M:%S%.3f', 
            time_zone='UTC',
            strict=False,
        )
    ).otherwise(pl.col('created_at').str.to_datetime(
        format="%Y-%m-%dT%H:%M:%S%.6f", 
        time_zone="UTC",
        strict=False,
    ))
)

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
    AB_DID = AB_DIDS[HANDLE] # get DID of attention broker
    # get all the attention broker's followers
    followers_of_ab = df_follows.filter(pl.col('to') == AB_DID)
    followed_by_ab = df_follows.filter(pl.col('from') == AB_DID)
    print(f'{HANDLE} follows {len(followed_by_ab)} accounts.')
    
    # load attention broker's reposts and create a polars dataframe
    reposts = json.load(open(f'{FILEPATH}/bsky_reposts/{HANDLE}.json', 'r'))
    reposts = [parse_repost_dict(r) for r in reposts]
    df_reposts = pl.DataFrame(reposts)
    df_reposts = df_reposts.with_columns(
        pl.col('created_at').str.to_datetime(
            format='%Y-%m-%dT%H:%M:%S%.3f%Z', 
            time_zone='UTC'
        )
    )
    # filter to reposts that are before the cutoff date
    df_reposts = df_reposts.filter(pl.col('created_at') <= REPOST_CUTOFF)
    # filter out self-reposts
    df_reposts = df_reposts.filter(pl.col('orig_poster') != AB_DID)
    # only analyze the first repost by the attention broker
    df_reposts = df_reposts.group_by(pl.col('orig_poster')).agg(pl.col('created_at').min())
    tot_reposts = len(df_reposts)
    
    # get earliest repost to make relative time_period column
    MIN_REPOST_DAY = df_reposts.select(pl.col('created_at')).min().item()
    print(MIN_REPOST_DAY)
    
    data_final = [] # will be used to create dataframe of DiD data.
    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        # this looks weird, but it means I can do polars dataframe math with repost_created_at
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
        repost_period = (repost_created_at.item() - MIN_REPOST_DAY).days # relative date of repost
        
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
        followed_sample = followed_to_sample_from.sample(n=n_controls, seed=42)

        for en, sample in enumerate(followed_sample.iter_rows(named=True)):
            # get all the follows to OP that could've happened in the time we observed
            follows_to_control = df_follows.filter(
                (pl.col('created_at') <= high_follow_bound) & \
                (pl.col('created_at') >= low_follow_bound) & \
                (pl.col('to') == sample['to'])
            )
            # populate with a column for when the repost was created
            follows_to_control = follows_to_control.with_columns(
                pl.lit(repost_created_at.item(), dtype=Datetime).alias('repost_created_at')
            )
            # join with attention broker follow information; a non-empty value V in created_at_from_ab 
            # indicates that an account that we know followed OP also followed the attention broker at time V.
            follows_to_control_following_ab = follows_to_control.join(
                followers_of_ab, 
                on='from', 
                how='left',
                suffix='_from_ab'
            )
            # created_at_from_ab is the time the follower --> attention broker tie formed
            # created_at is the time the follower --> reposted acct tie formed
        
            # first, figure out when the follower --> reposted tie happened relative to the repost
            # pl.col('whatever1').sub(pl.col('whatever2')) subtracts the values in whatever2 from the values in whatever1.
            follows_to_control_following_ab = follows_to_control_following_ab.with_columns(
                ((pl.col('created_at').sub(pl.col('repost_created_at'))).dt.total_hours().floordiv(24)).alias('days_before_after_repost'),
                (pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))),
            )
            # obtain all follow events prior to repost
            followers_before_repost = follows_to_control_following_ab.filter(
                pl.col('days_before_after_repost') < 0
            )
            # obtain all follow events after repost
            followers_after_repost = follows_to_control_following_ab.filter(
                pl.col('days_before_after_repost') >= 0
            )
    
            # figure out who is a follower of the attention broker and was therefore "treated" at the time they followed OP
            followers_before_repost = followers_before_repost.with_columns(
                ((pl.col('created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
            )
            followers_after_repost = followers_after_repost.with_columns(
                ((pl.col('repost_created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
            )
    
            # obtain per-day total follow counts
            followers_before_repost = followers_before_repost.group_by(
                [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())
            followers_after_repost = followers_after_repost.group_by(
                [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())

            # add to dataset
            for row in followers_before_repost.iter_rows(named=True):
                data_final.append({
                    'gain_rate': row['from'],
                    'ever_treated': row['ab_follower'],
                    'unit_id': tot_reposts + en + (ix * n_controls),
                    'time_period': repost_period + row['days_before_after_repost'],
                    'ts': row['days_before_after_repost'],
                })
        
            for row in followers_after_repost.iter_rows(named=True):
                data_final.append({
                    'gain_rate': row['from'],
                    'ever_treated': row['ab_follower'],
                    'unit_id': tot_reposts + en + (ix * n_controls),
                    'time_period': repost_period + row['days_before_after_repost'],
                    'ts': row['days_before_after_repost'],
                })
            
    # build dataframe from list of dicts
    data = pl.DataFrame(data_final)    
    fwd_str = str(DAYS_FWD)
    bwd_str = str(DAYS_BWD)
    data.write_csv(f'{FILEPATH_OUT}/control_csvs/{HANDLE}_fwd_{DAYS_FWD}_bwd_{DAYS_BWD}.csv')
    print('done')

for AB_HANDLE in AB_HANDLES:
    make_control_csv(AB_HANDLE, df_follows, DAYS_FWD, DAYS_BWD)
# DID_FILES = os.listdir(f'{FILEPATH}/did_csvs')
# PROCESSED_HANDLES = set([d[:-4] for d in DID_FILES])
# for handle in list(AB_DIDS.keys()):
#     print(handle)
#     if handle not in PROCESSED_HANDLES:
#         make_did_csv(handle, df_follows)
