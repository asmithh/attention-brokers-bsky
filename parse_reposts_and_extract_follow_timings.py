import datetime as dt
import gc
import json
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
"""

FILEPATH = '/scratch/nte5cp' # change this for your machine
AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))

# set conservative upper bound on repost events we'll study.
REPOST_CUTOFF = dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))


df_follows = pl.read_csv(
    f'{FILEPATH}/follows_all.csv', 
    has_header=False, 
    new_columns=["from", "to", "created_at"],
)
df_follows = df_follows.drop_nulls()
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

def make_did_csv(HANDLE, df_follows):
    AB_DID = AB_DIDS[HANDLE]
    followers_of_ab = df_follows.filter(pl.col('to') == AB_DID)
    reposts = json.load(open(f'{FILEPATH}/bsky_reposts/{HANDLE}.json', 'r'))
    reposts = [parse_repost_dict(r) for r in reposts]
    df_reposts = pl.DataFrame(reposts)
    df_reposts = df_reposts.with_columns(
        pl.col('created_at').str.to_datetime(
            format='%Y-%m-%dT%H:%M:%S%.3f%Z', 
            time_zone='UTC'
        )
    )
    df_reposts = df_reposts.filter(pl.col('created_at') <= REPOST_CUTOFF)
    df_reposts = df_reposts.filter(pl.col('orig_poster') != AB_DID)
    df_reposts = df_reposts.group_by(pl.col('orig_poster')).agg(pl.col('created_at').min())
    
    MIN_REPOST_DAY = df_reposts.select(pl.col('created_at')).min().item()
    print(MIN_REPOST_DAY)
    
    data_final = []
    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
        repost_period = (repost_created_at.item() - MIN_REPOST_DAY).days
        
        orig_poster = row['orig_poster']
        low_follow_bound = row['created_at'] - dt.timedelta(days=14)
        high_follow_bound = row['created_at'] + dt.timedelta(days=14)
    
        follows_to_op = df_follows.filter(
            (pl.col('created_at') <= high_follow_bound) & \
            (pl.col('created_at') >= low_follow_bound) & \
            (pl.col('to') == orig_poster)
        )
        follows_to_op = follows_to_op.with_columns(
            pl.lit(repost_created_at.item(), dtype=Datetime).alias('repost_created_at')
        )
        follows_to_op_following_ab = follows_to_op.join(
            followers_of_ab, 
            on='from', 
            how='left',
            suffix='_from_ab'
        )
        # created_at_from_ab is the time the follower --> attention broker tie formed
        # created_at is the time the follower --> reposted acct tie formed
    
        # first, figure out when the follower --> reposted tie happened relative to the repost
        follows_to_op_following_ab = follows_to_op_following_ab.with_columns(
            ((pl.col('repost_created_at').sub(pl.col('created_at'))).dt.total_days()).alias('days_before_after_repost'),
            (pl.col('created_at_from_ab').fill_null(repost_created_at.item()))
        )
        followers_before_repost = follows_to_op_following_ab.filter(
            pl.col('days_before_after_repost') < 0
        )
        followers_after_repost = follows_to_op_following_ab.filter(
            pl.col('days_before_after_repost') >= 0
        )
    
        followers_before_repost = followers_before_repost.with_columns(
            ((pl.col('created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
        )
        followers_after_repost = followers_after_repost.with_columns(
            ((pl.col('repost_created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
        )
    
        followers_before_repost = followers_before_repost.group_by(
            [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())
        followers_after_repost = followers_after_repost.group_by(
            [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())
        
        for row in followers_before_repost.iter_rows(named=True):
            data_final.append({
                'gain_rate': row['from'],
                'ever_treated': row['ab_follower'],
                'unit_id': ix,
                'time_period': repost_period + row['days_before_after_repost'],
                'ts': row['days_before_after_repost'],
            })
    
        for row in followers_after_repost.iter_rows(named=True):
            data_final.append({
                'gain_rate': row['from'],
                'ever_treated': row['ab_follower'],
                'unit_id': ix,
                'time_period': repost_period + row['days_before_after_repost'],
                'ts': row['days_before_after_repost'],
            })
    
    data = pl.DataFrame(data_final)    
    data.write_csv(f'{FILEPATH}/did_csvs/{HANDLE}.csv')
    print('done')


for handle in list(AB_DIDS.keys())[4:]:
    print(handle)
    make_did_csv(handle, df_follows)
