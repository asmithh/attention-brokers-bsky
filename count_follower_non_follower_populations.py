import datetime as dt
import gc
import json
import random
import sys
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import polars as pl
from polars.datatypes import String, Int64, Datetime

from utils import *
"""
Used to count all followers and non-followers who followed at least one account reposted by an attention broker; 
outputs a JSON file with keys 'ab_followers' and 'non_followers' corresponding to the number of unique
attention broker followers and unique non-followers observed in the dataset.

Run as python3 count_follower_non_follower_populations.py $HANDLE_INF $DAYS_FWD $DAYS_BWD
HANDLE_INF is the .txt file of Bluesky handles of attention brokers
DAYS_FWD is the number of days for which we have data after the repost
DAYS_BWD is the number of days for which we have data before the repost
"""

HANDLE_INF = sys.argv[1]
DAYS_FWD = int(sys.argv[2])
DAYS_BWD = int(sys.argv[3])

HANDLES = []
with open(HANDLE_INF, 'r') as f:
    for line in f.readlines():
        HANDLES.append(line.strip())
        
FILEPATH = '/scratch/nte5cp' # change this for your machine
AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))

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

def count_populations(HANDLE, df_follows, days_fwd, days_bwd):
    """
    Make capture histories for followers and non-followers of an attention broker.
    Write capture histories to a file in the format MARK expects.

    Inputs:
        HANDLE: attention broker's Bluesky handle
        df_follows: polars dataframe of follow events w/ columns 
            from (follower), to (followed), and created_at (follow timestamp)

    Outputs:
        No output variables; writes output to a .txt file
    """
    AB_DID = AB_DIDS[HANDLE] # get DID of attention broker
    # get all the attention broker's followers
    followers_of_ab = df_follows.filter(pl.col('to') == AB_DID) 

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

    ab_followers_following = set() # followers of the attention broker who followed a reposted account at least once
    non_followers_following = set() # accounts not following the attention broker who followed a reposted account at least once
    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        # this looks weird, but it means I can do polars dataframe math with repost_created_at
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]}) 
        
        orig_poster = row['orig_poster'] # referred to as OP (original poster) in these comments
        low_follow_bound = row['created_at'] - dt.timedelta(days=days_bwd) # the minimum day we will collect following data for
        high_follow_bound = row['created_at'] + dt.timedelta(days=days_fwd) # the maximum day we will collect following data for

        # get all the follows to OP that could've happened in the time we observed
        follows_to_op = df_follows.filter(
            (pl.col('created_at') <= high_follow_bound) & \
            (pl.col('created_at') >= low_follow_bound) & \
            (pl.col('to') == orig_poster)
        )
        follows_to_op = follows_to_op.with_columns(
            pl.lit(repost_created_at.item(), dtype=Datetime).alias('repost_created_at')
        )
        # join with attention broker follow information; a non-empty value V in created_at_from_ab 
        # indicates that an account that we know followed OP also followed the attention broker at time V.
        follows_to_op_following_ab = follows_to_op.join(
            followers_of_ab, 
            on='from', 
            how='left',
            suffix='_from_ab'
        )
        # created_at_from_ab is the time the follower --> attention broker tie formed
        # created_at is the time the follower --> OP tie formed
    
        # first, figure out when the follower --> reposted tie happened relative to the repost
        # pl.col('whatever1').sub(pl.col('whatever2')) subtracts the values in whatever2 from the values in whatever1.
        follows_to_op_following_ab = follows_to_op_following_ab.with_columns(
            ((pl.col('repost_created_at').sub(pl.col('created_at'))).dt.total_days()).alias('days_before_after_repost'),
            (pl.col('created_at_from_ab').fill_null(repost_created_at.item()))
        )
        # next, figure out who is a follower of the attention broker (i.e. in the treatment group)
        follows_to_op_following_ab = follows_to_op_following_ab.with_columns(
            pl.when(pl.col('days_before_after_repost') >= 0).then(
                pl.col('created_at').sub(pl.col('created_at_from_ab')).dt.total_seconds() > 0
            ).otherwise(
                pl.col('repost_created_at').sub(pl.col('created_at_from_ab')).dt.total_seconds() > 0).alias('ab_follower')
        )
        # keep track of who is an attention broker follower and who is not; add to the sets of known individuals.
        ab_followers_following = ab_followers_following | \
            set(follows_to_op_following_ab.filter(pl.col('ab_follower') == True)['from'].to_numpy().tolist())
        non_followers_following = non_followers_following | \
            set(follows_to_op_following_ab.filter(pl.col('ab_follower') == False)['from'].to_numpy().tolist())

    # count unique followers and non-followers seen in the dataset.
    res = {'ab_followers': len(ab_followers_following), 'non_followers': len(non_followers_following)}
    # jump to JSON file
    json.dump(res, open(f'{FILEPATH}/population_counts/{HANDLE}_fwd_{days_fwd}_bwd_{days_bwd}.json', 'w'))

# currently we've written the capture history for Jorts only.
for handle in HANDLES:
    print(handle)
    count_populations(handle, df_follows, DAYS_FWD, DAYS_BWD)