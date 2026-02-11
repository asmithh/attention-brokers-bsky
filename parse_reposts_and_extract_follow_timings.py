import datetime as dt
import gc
import json
from zoneinfo import ZoneInfo

import polars as pl
from polars.datatypes import String, Int64, Datetime

AB_DIDS = json.load(open('handles_to_dids.json', 'r'))

# FILEPATH = '/scratch/nte5cp'
FILEPATH = '/Users/a404/attention-brokers-bsky/'
HANDLE = 'jortsthecat.bsky.social'
AB_DID = AB_DIDS[HANDLE]
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

# df_follows = pl.read_csv(f'{FILEPATH}/follows.csv', header=False, new_columns=["from", "to", "created_at"])

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
df_reposts = df_reposts.group_by(pl.col('reposted')).agg(pl.col('created_at').min())

MIN_REPOST_DAY = df_reposts.select(pl.col('created_at')).min() 

data_final = []
for ix, row in enumerate(df_reposts.iter_rows(names=True)):
    repost_created_at = row['created_at']
    repost_period = (repost_created_at - MIN_REPOST_DAY).days

    orig_poster = row['orig_poster']
    low_follow_bound = row['created_at'] - dt.timedelta(days=14)
    high_follow_bound = row['created_at'] + dt.timedelta(days=14)

    follows_to_op = df_follows.filter(
        (pl.col('created_at') <= high_follow_bound) & \
        (pl.col('created_at') >= low_follow_bound) & \
        (pl.col('to') == orig_poster)
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
        ((repost_created_at - pl.col('created_at')).days).alias('days_before_after_repost')
    )

    followers_before_repost = follows_to_op_following_ab.filter(
        pl.col('days_before_after_repost') < 0
    )
    followers_after_repost = follows_to_op_following_ab.filter(
        pl.col('days_before_after_repost') >= 0
    )

    followers_before_repost.with_column(
        ((pl.col('created_at') - pl.col('created_at_from_ab')).total_seconds() > 0).alias('ab_follower')
    )
    followers_after_repost.with_column(
        ((repost_created_at - pl.col('created_at_from_ab')).total_seconds() > 0).alias('ab_follower')
    )

    followers_before_repost = followers_before_repost.group_by(
        [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())
    followers_after_repost = followers_after_repost.group_by(
        [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())
    
    for row in followers_before_repost.iter_rows():
        if row['ab_follower']:
            ts = row['days_before_after_repost']
        else:
            ts = np.inf
        data_final.append({
            'gain_rate': row['from'],
            'treated': 0,
            'unit_id': ix,
            'time_period': repost_period + row['days_before_after_repost'],
            'ts': ts,
        })

    for row in followers_after_repost.iter_rows():
        if row['ab_follower']:
            ts = row['days_before_after_repost']
        else:
            ts = np.inf
        data_final.append({
            'gain_rate': row['from'],
            'treated': row['ab_follower'],
            'unit_id': ix,
            'time_period': repost_period + row['days_before_after_repost'],
            'ts': ts,
        })

data = pl.DataFrame(data_final)    
data.write_csv(f'did_csvs/{HANDLE}.csv')


    
    
