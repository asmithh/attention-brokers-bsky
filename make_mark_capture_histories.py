import datetime as dt
import gc
import json
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import polars as pl
from polars.datatypes import String, Int64, Datetime

FILEPATH = '/scratch/nte5cp'
AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))


# FILEPATH = '/Users/a404/attention-brokers-bsky/'

REPOST_CUTOFF = dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

def extract_did_from_uri(uri):
    uri_split_slashes = uri.split('/')
    return uri_split_slashes[2]

def parse_repost_dict(repost_dict):
    reposter = extract_did_from_uri(repost_dict['uri'])
    orig_poster = extract_did_from_uri(repost_dict['reposted'])
    created_at = repost_dict['created-at']
    
    return {
        'reposter': reposter,
        'orig_poster': orig_poster,
        'created_at': created_at.replace('Z', '+00:00'),
    }

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


def make_mark_data(HANDLE, df_follows):
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
    MAX_REPOST_DAY = df_reposts.select(pl.col('created_at')).max().item()
    
    original_posters = set()
    ab_followers_following = set()
    non_followers_following = set()
    for ix, row in enumerate(df_reposts.iter_rows(named=True)):
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
        repost_period = (repost_created_at.item() - MIN_REPOST_DAY).days
        
        orig_poster = row['orig_poster']
        original_posters.add(orig_poster)
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
        follows_to_op_following_ab = follows_to_op_following_ab.with_columns(
            pl.when(pl.col('days_before_after_repost') >= 0).then(
                pl.col('created_at').sub(pl.col('created_at_from_ab')).dt.total_seconds() > 0
            ).otherwise(
                pl.col('repost_created_at').sub(pl.col('created_at_from_ab')).dt.total_seconds() > 0).alias('ab_follower')
        )
        ab_followers_following = ab_followers_following | \
            set(follows_to_op_following_ab.filter(pl.col('ab_follower') == True)['from'].to_numpy().tolist())
        non_followers_following = non_followers_following | \
            set(follows_to_op_following_ab.filter(pl.col('ab_follower') == False)['from'].to_numpy().tolist())
    
    
    pl.col("sets").list.set_intersection([1,7]).list.len() != 0
    follow_time_range = pd.date_range(
        start=MIN_REPOST_DAY.replace(hour=0, minute=0, second=0, microsecond=0) - dt.timedelta(days=14),
        end=MAX_REPOST_DAY.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=14),
        freq='1d')
    original_posters = list(original_posters)
    follow_time_mapping = {day: ix for ix, day in enumerate(follow_time_range)}
    
    for acct_set, set_name in (
        (ab_followers_following, 'ab_followers'),
        (non_followers_following, 'non_followers')
    ):
        acct_set = list(acct_set)
        accts_that_followed_rted_acct = df_follows.filter(
            pl.col('from').is_in(acct_set),
            pl.col('to').is_in(original_posters),
            pl.col('created_at') >= MIN_REPOST_DAY - dt.timedelta(days=14),
            pl.col('created_at') <= MAX_REPOST_DAY + dt.timedelta(days=14),
        )
        accts_that_followed_rted_acct = accts_that_followed_rted_acct.with_columns(
            pl.col('created_at').dt.truncate('1d').alias('created_at_floor_day'),
        )
    
        followers_by_days_followed = accts_that_followed_rted_acct.group_by('from').agg(pl.col('created_at_floor_day').unique())
        
        with open(f'{FILEPATH}/mark_data/{HANDLE}_{set_name}.txt', 'w') as fout:
            for row in followers_by_days_followed.iter_rows(named=True):
                vec = np.zeros(len(follow_time_mapping))
                for day in row['created_at_floor_day']:
                    vec[follow_time_mapping[day]] = 1
                fout.write(''.join([str(int(vv)) for vv in vec]) + ';\n')  

for handle in list(AB_DIDS.keys())[1:]:
    print(handle)
    make_mark_data(handle, df_follows)