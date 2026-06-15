"""
Code to get per-k-hours total follower accumulation counts for reposted and control accounts
from both attention broker followers and non-followers.

Run this as python3 get_control_and_reposted_full_trajectories.py HANDLES_IN HRS_PERIOD, e.g.

python3 get_control_and_reposted_full_trajectories.py ab_handles.txt 1

HANDLES_IN should be the relative or absolute path to a file 
containing a list of Bluesky handles, one per line. 

It's more efficient to process multiple handles in one go because loading the follows graph
is very time-consuming and RAM-intensive.

HRS_PERIOD should be a positive integer number.
The smaller HRS_PERIOD gets, the larger the data files become; 
1 hour --> expect file sizes in the 100s of MBs. 
"""

import datetime as dt
import json
import sys

import polars as pl

from config import FILEPATH, FILEPATH_OUT, AB_DIDS, REPOST_CUTOFF
from utils import *

# get potential attention broker handles
AB_HANDLES = []
with open(sys.argv[1], 'r') as f:
    for line in f.readlines():
        AB_HANDLES.append(line.strip())

HRS_PERIOD = int(sys.argv[2]) # change later to be a float? 

# load follows graph; note this uses 100s of GBs of RAM and takes at least 30 min.
df_follows = load_df_follows(FILEPATH) 

for handle in AB_HANDLES:
    print(handle, flush=True)
    ab_did = AB_DIDS[handle] # look up DID of handle

    # grab reposts from the attention broker
    df_reposts, min_repost_day, tot_reposts, reposted_before = make_repost_df(FILEPATH, handle, ab_did)
    followers_of_ab, followed_by_ab, reposted_accts_followed_by_ab, control_accts, accts_to_unit_id = get_followed_accts_and_unit_ids_with_delineation(
        df_follows, 
        handle, 
        ab_did, 
        df_reposts,
        reposted_before,
    )

    # open file you'll write to
    with open (f'{FILEPATH}/total_follower_accumulation_data/{handle}_follows_to_ops_and_never_reposted_controls_period_{HRS_PERIOD}_hrs.csv', 'w') as outf:
        outf.write('unit_id,period,ever_treated,period_treated,tot_ab_fol,tot_non_fol\n') # add column names

        # track population of unique followers and non-followers
        set_of_ab_followers = set() 
        set_of_non_followers = set()

        # especially important to know up front that we have enough controls.
        print(f'reposts: {len(df_reposts)}', flush=True)
        print(f'controls: {len(control_accts)}', flush=True)

        # first we collate data for the reposted accounts
        for row in df_reposts.iter_rows(named=True):
            orig_poster = row['orig_poster']

            # ignore accts not followed by the attention broker.
            if orig_poster not in accts_to_unit_id:
                continue

            # compute number of n-hour periods elapsed from earliest repost until this repost.
            repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
            repost_period = (repost_created_at.item() - min_repost_day).total_seconds() // (60 * 60 * HRS_PERIOD)

            # filter down follows to the OP to be in the period of interest.
            follows_to_op = df_follows.filter(
                (pl.col('created_at') <= REPOST_CUTOFF) & \
                (pl.col('created_at') >= min_repost_day) & \
                (pl.col('to') == orig_poster)
            )
            # add the time the repost was created at as a column for easier subtraction later.
            follows_to_op = follows_to_op.with_columns(
                pl.lit(repost_created_at.item(), dtype=Datetime).alias('repost_created_at')
            )
            # join with followers of AB so we can find out if/when the user followed the AB.
            follows_to_op = follows_to_op.join(
                followers_of_ab, 
                on='from', 
                how='left',
                suffix='_from_ab'
            )
            # fill in the empty dates for users who didn't follow the attention broker to be far in the future.
            follows_to_op = follows_to_op.with_columns(
                pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))
            )
            # calculate how many periods elapsed before they followed the attention broker and the OP. this could be negative.
            follows_to_op = follows_to_op.with_columns(
                pl.col('created_at_from_ab').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_ab'),
                pl.col('created_at').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_op')
            )
            # filter for whether we count each follower to OP as a follower of the attention broker.
            follows_to_op = follows_to_op.with_columns(
                (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_op')).alias('followed_ab_before_op'),
            )
            # update our population of attention broker followers and non-followers.
            set_of_ab_followers |= set(follows_to_op.filter(pl.col('followed_ab_before_op') == 1)['from'].to_list())
            set_of_non_followers |= set(follows_to_op.filter(pl.col('followed_ab_before_op') == 0)['from'].to_list())

            # get new follows that occurred each period (n hours) for both followers and non-followers of the AB (separately).
            per_day_ab_follower_follows_to_op = follows_to_op.filter(
                pl.col('followed_ab_before_op') == 1
            ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_op'))
            per_day_non_ab_follower_follows_to_op = follows_to_op.filter(
                pl.col('followed_ab_before_op') == 0
            ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_op'))

            # get total number of periods elapsed in this dataset.
            tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (HRS_PERIOD * 60 * 60))
            # make a dict that tells us how many follows occurred in each period from AB followers and non-followers
            # note that the default value is zero.
            follows_per_period = {d: {'ab_fol': 0, 'non_fol': 0} for d in range(tot_periods + 1)}

            for df, label in [
                (per_day_ab_follower_follows_to_op, 'ab_fol'), 
                (per_day_non_ab_follower_follows_to_op, 'non_fol')
            ]:
                for row in df.iter_rows(named=True):
                    follows_per_period[row['periods_until_followed_op']][label] = row['new_follows_per_period']
            
            # set up running total of accumulated follows from AB followers and non-followers
            tot_ab_fol = 0
            tot_non_fol = 0
            for k, v in sorted(follows_per_period.items(), key=lambda b: b[0]):
                """
                writes the following items, comma-separated on one CSV line:
                * unit_id: unique integer ID for the reposted account, 
                * time_period: number of n-hour periods elapsed since first repost,
                * ever_treated (always 1 for reposted accounts),
                * period_treated: number of n-hour periods elapsed when treatment occurred,
                * tot_ab_fol: total follows accumulated from AB followers,
                * tot_non_fol: total follows accumulated from AB non-followers
                """
                outf.write(f'{accts_to_unit_id[orig_poster]},{k},1,{repost_period},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
                tot_ab_fol += v['ab_fol']
                tot_non_fol += v['non_fol']

        for control in list(control_accts):
            # filter down follows to the control acct to be in the period of interest.
            follows_to_control = df_follows.filter(
                (pl.col('created_at') <= REPOST_CUTOFF) & \
                (pl.col('created_at') >= min_repost_day) & \
                (pl.col('to') == control)
            )

            # join with followers of AB so we can find out if/when the user followed the AB.
            follows_to_control = follows_to_control.join(
                followers_of_ab, 
                on='from', 
                how='left',
                suffix='_from_ab'
            )
            # fill in the empty dates for users who didn't follow the attention broker to be far in the future.
            follows_to_control = follows_to_control.with_columns(
                pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))
            )
            # get new follows that occurred each period (n hours) for both followers and non-followers of the AB (separately).
            follows_to_control = follows_to_control.with_columns(
                pl.col('created_at_from_ab').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_ab'),
                pl.col('created_at').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_control')
            )
            # filter for whether we count each follower to the control acct as a follower of the attention broker.

            follows_to_control = follows_to_control.with_columns(
                (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_control')).alias('followed_ab_before_control'),
            )
            # update our population of attention broker followers and non-followers.
            set_of_ab_followers |= set(follows_to_control.filter(pl.col('followed_ab_before_control') == 1)['from'].to_list())
            set_of_non_followers |= set(follows_to_control.filter(pl.col('followed_ab_before_control') == 0)['from'].to_list())

            # get new follows that occurred each period (n hours) for both followers and non-followers of the AB (separately).
            per_day_ab_follower_follows_to_control = follows_to_control.filter(
                pl.col('followed_ab_before_control') == 1
            ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_control'))
            per_day_non_ab_follower_follows_to_control = follows_to_control.filter(
                pl.col('followed_ab_before_control') == 0
            ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_control'))

            # get total number of periods elapsed in this dataset.
            tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (HRS_PERIOD * 60 * 60))
            # make a dict that tells us how many follows occurred in each period from AB followers and non-followers
            # note that the default value is zero.
            follows_per_period = {d: {'ab_fol': 0, 'non_fol': 0} for d in range(tot_periods + 1)}
            for df, label in [
                (per_day_ab_follower_follows_to_control, 'ab_fol'), 
                (per_day_non_ab_follower_follows_to_control, 'non_fol')
            ]:
                for row in df.iter_rows(named=True):
                    follows_per_period[row['periods_until_followed_control']][label] = row['new_follows_per_period']
            
        
            tot_ab_fol = 0
            tot_non_fol = 0
            for k, v in sorted(follows_per_period.items(), key=lambda b: b[0]):
                """
                writes the following items, comma-separated on one CSV line:
                * unit_id: unique integer ID for the reposted account, 
                * time_period: number of n-hour periods elapsed since first repost,
                * ever_treated (always 0 for control accounts),
                * period_treated: number of n-hour periods elapsed when treatment occurred,
                * tot_ab_fol: total follows accumulated from AB followers,
                * tot_non_fol: total follows accumulated from AB non-followers
                """
                outf.write(f'{accts_to_unit_id[control]},{k},0,{10000},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
                tot_ab_fol += v['ab_fol']
                tot_non_fol += v['non_fol']
    
    # dump population numbers to a JSON blob.
    populations = {'ab_fol': len(set_of_ab_followers), 'non_fol': len(set_of_non_followers)}
    json.dump(populations, open(f'{FILEPATH_OUT}/population_counts/{handle}_new_controls_population_count_panel.json', 'w'))