import datetime as dt
import gc
import json
import random
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import polars as pl
from polars.datatypes import String, Int64, Datetime

from utils import *
"""
Used to make a mark-recapture history for all accounts observed in the dataset; 
the resulting .txt file is for MARK to estimate the population of attention broker followers and non-followers.

Capture histories are strings of length n_observations and have a 1 if the individual was observed on that day
and a 0 otherwise. Each account's capture history is demarcated in the .txt file by a semicolon and newline character.
"""

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

def make_mark_data(HANDLE, df_follows):
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

    # get earliest and latest reposts so we can figure out how long our capture histories are (later).
    MIN_REPOST_DAY = df_reposts.select(pl.col('created_at')).min().item()
    print(MIN_REPOST_DAY)
    MAX_REPOST_DAY = df_reposts.select(pl.col('created_at')).max().item()

    original_posters = set() # original posters -- reposted by attention broker
    ab_followers_following = set() # followers of the attention broker who followed a reposted account at least once
    non_followers_following = set() # accounts not following the attention broker who followed a reposted account at least once
    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        # this looks weird, but it means I can do polars dataframe math with repost_created_at
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]}) 
        
        orig_poster = row['orig_poster'] # referred to as OP (original poster) in these comments
        original_posters.add(orig_poster)
        low_follow_bound = row['created_at'] - dt.timedelta(days=14) # the minimum day we will collect following data for
        high_follow_bound = row['created_at'] + dt.timedelta(days=14) # the maximum day we will collect following data for

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

    # range of times we could've seen a follow event in our dataset.
    # polars date_range is uncooperative wrt enumeration, so that's why we're using the pandas date_range.
    follow_time_range = pd.date_range(
        start=MIN_REPOST_DAY.replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=14),
        end=MAX_REPOST_DAY.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=14),
        freq='1d')
    original_posters = list(original_posters)
    # maps whole days in follow_time_range to their index in follow_time_range.
    follow_time_mapping = {day: ix for ix, day in enumerate(follow_time_range)}
    
    for acct_set, set_name in (
        (ab_followers_following, 'ab_followers'),
        (non_followers_following, 'non_followers')
    ):
        # take 10% random sample of accounts so that RMark can handle the input.
        if random.randint(0, 10) != 5:
            continue
        acct_set = list(acct_set)
        # obtain all follow events that occurred from an account 
        # in the set of either {attention broker followers, attention broker non-followers}
        # and was to one of the reposted accounts in our dataset.
        # these should also have taken place in the period we're studying.
        accts_that_followed_rted_acct = df_follows.filter(
            pl.col('from').is_in(acct_set),
            pl.col('to').is_in(original_posters),
            pl.col('created_at') >= MIN_REPOST_DAY - dt.timedelta(days=14),
            pl.col('created_at') <= MAX_REPOST_DAY + dt.timedelta(days=14),
        )
        # truncate follow timings to the nearest day for per-day binning.
        accts_that_followed_rted_acct = accts_that_followed_rted_acct.with_columns(
            pl.col('created_at').dt.truncate('1d').alias('created_at_floor_day'),
        )

        # for each account A in the set of either {attention broker followers, attention broker non-followers}
        # produce a list of days on which we observe A following a reposted account.
        followers_by_days_followed = accts_that_followed_rted_acct.group_by('from').agg(pl.col('created_at_floor_day').unique())
        
        with open(f'{FILEPATH}/mark_data/{HANDLE}_{set_name}_ten_pct.txt', 'w') as fout:
            for row in followers_by_days_followed.iter_rows(named=True):
                # for each account A in this set of users, we write a line in fout to represent their capture history.
                vec = np.zeros(len(follow_time_mapping)) # make an array of zeros, one for each day of the period we look at
                for day in row['created_at_floor_day']:
                    vec[follow_time_mapping[day]] = 1 # flip the entry for any day where a follow from A occurred to 1
                fout.write(''.join([str(int(vv)) for vv in vec]) + ';\n')  

# currently we've written the capture history for Jorts only.
for handle in list(AB_DIDS.keys())[::-1]:
    print(handle)
    make_mark_data(handle, df_follows)