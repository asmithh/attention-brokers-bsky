import datetime as dt
import json
import os
import random
import sys

import pandas as pd
import polars as pl

from config import FILEPATH, FILEPATH_OUT, REPOST_CUTOFF
from utils import *

random.seed(42) # set random seed for reproducible sub-sampling when there are too many reposts or controls

ab_row_num = int(sys.argv[1]) # first command line argument; indicates account number for slurm jobarray purposes

df_cand = pd.read_csv(f'{FILEPATH_OUT}/attention-brokers-bsky/candidates_with_dids_by_20250915.csv')
ab_row = df_cand.iloc[ab_row_num]
ab_did = ab_row['did']
ab_ix = ab_row['id']
print('ab ix: ', ab_ix, flush=True)
print('loaded df cand', flush=True)

# keep track of accounts whose data we've successfully FULLY processed
finished = set()
with open(f'{FILEPATH_OUT}/attention-brokers-bsky/finished_anon_ids.txt', 'r') as f:
    for line in f.readlines():
        try:
            finished.add(int(line.strip()))
        except Exception as e:
            print(e, flush=True)

if ab_ix in finished:
    print(f"already seen {ab_ix}")
    sys.exit(1)
print('checked ab ids', flush=True)

HRS_PERIOD = float(sys.argv[2]) # second command line argument; indicates how many hours over which to aggregate follower accumulation

TESTING = int(sys.argv[3]) # third command line argument; indicates whether we should test code on a subset of the follower network data

REWRITE_FILES = int(sys.argv[4]) # fourth command line argument; indicates whether we should overwrite any existing files for this account.

print('loading df follows', flush=True)
df_follows = load_df_follows(FILEPATH, testing=TESTING) # takes some time to run and may consume >600GB RAM

df_reposts, min_repost_day, tot_reposts, reposted_before = make_repost_df(FILEPATH_OUT, ab_ix, ab_did)

followers_of_ab, followed_by_ab, reposted_accts_followed_by_ab, control_accts, accts_to_unit_id = get_followed_accts_and_unit_ids_with_delineation(
    df_follows, 
    ab_ix, 
    ab_did, 
    df_reposts,
    reposted_before,
)

if TESTING:
    fname_out = f'{FILEPATH_OUT}/attention-brokers-bsky/panel_data/TEST_{ab_ix}_follows_to_ops_and_never_reposted_controls_period_{HRS_PERIOD}_hrs.csv'
else:
    fname_out = f'{FILEPATH_OUT}/attention-brokers-bsky/panel_data/{ab_ix}_follows_to_ops_and_never_reposted_controls_period_{HRS_PERIOD}_hrs.csv'

# if we're running a job that got killed due to time limit, we want to append our information to the existing file.
if os.path.exists(fname_out) and REWRITE_FILES == 0:
    edit_flag = 'a'
    os.system(f'cp {fname_out} {fname_out[:-4]}_TMP_OLD.csv')
    old_df = pd.read_csv(f'{fname_out[:-4]}_TMP_OLD.csv')
    seen_units =  set(old_df['unit_id'].unique().tolist())

else:
    edit_flag = 'w'
    seen_units = set()
    

with open (fname_out, edit_flag) as outf:
    if edit_flag == 'w':
        outf.write('unit_id,period,ever_treated,period_treated,tot_ab_fol,tot_non_fol\n') # write header line if new file
    print(f'reposts: {len(df_reposts)}', flush=True)
    print(f'controls: {len(control_accts)}', flush=True)

    # no reposts --> no analysis downstream
    if len(df_reposts) == 0:
        print('no reposts', flush=True)
        with open(f'{FILEPATH_OUT}/attention-brokers-bsky/finished_anon_ids.txt', 'a') as f_fin:
            f_fin.write(str(ab_ix) + '\n')
        sys.exit(1)
    # downsample reposts for purposes of scalability
    elif len(df_reposts) > 5000:
        df_reposts = df_reposts.sample(n=5000, seed=42)

    # downsample controls for purposes of scalability
    if len(control_accts) > 5000:
        control_accts = set(random.sample(list(control_accts), 5000))
        
    for row in df_reposts.iter_rows(named=True):
        orig_poster = row['orig_poster']
        # avoid writing duplicate data to file or working with accounts that weren't followed by the attention broker.
        if orig_poster not in accts_to_unit_id or accts_to_unit_id[orig_poster] in seen_units:
            continue
        repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
        # if repost happened before the AB followed OP, we don't want to count this repost.
        if row['created_at'] < followed_by_ab.filter(pl.col('to') == orig_poster)['created_at'].to_list()[0]:
            print('followed after repost', flush=True)
            continue
        repost_period = (repost_created_at.item() - min_repost_day).total_seconds() // (60 * 60 * HRS_PERIOD)

        # get follow events to content's original poster (OP)
        follows_to_op = df_follows.filter(
            (pl.col('created_at') <= REPOST_CUTOFF) & \
            (pl.col('created_at') >= min_repost_day) & \
            (pl.col('to') == orig_poster)
        )
        follows_to_op = follows_to_op.with_columns(
            pl.lit(repost_created_at.item(), dtype=Datetime).alias('repost_created_at')
        )
        follows_to_op = follows_to_op.join(
            followers_of_ab, 
            on='from', 
            how='left',
            suffix='_from_ab'
        )
        # figure out which follows to OP came from followers of the attention broker (AB)
        follows_to_op = follows_to_op.with_columns(
            pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))
        )
        follows_to_op = follows_to_op.with_columns(
            pl.col('created_at_from_ab').sub(
                pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_ab'),
            pl.col('created_at').sub(
                pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_op')
        )
        # check if AB followers followed AB before they followed OP; if so, they count as followers of the AB
        follows_to_op = follows_to_op.with_columns(
            (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_op')).alias('followed_ab_before_op'),
        )

        # count AB follower + AB non-follower follows to OP
        per_day_ab_follower_follows_to_op = follows_to_op.filter(
            pl.col('followed_ab_before_op') == 1
        ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
            by=pl.col('periods_until_followed_op'))
        per_day_non_ab_follower_follows_to_op = follows_to_op.filter(
            pl.col('followed_ab_before_op') == 0
        ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
            by=pl.col('periods_until_followed_op'))

        # get the period since the beginning of our earliest repost
        tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (HRS_PERIOD * 60 * 60))
        follows_per_period = {d: {'ab_fol': 0, 'non_fol': 0} for d in range(tot_periods + 1)}
        for df, label in [
            (per_day_ab_follower_follows_to_op, 'ab_fol'), 
            (per_day_non_ab_follower_follows_to_op, 'non_fol')
        ]:
            for row in df.iter_rows(named=True):
                follows_per_period[row['periods_until_followed_op']][label] = row['new_follows_per_period']
        
        # write per-period follower accumulation figures for followers and non-followers.
        tot_ab_fol = 0
        tot_non_fol = 0
        for k, v in sorted(follows_per_period.items(), key=lambda b: b[0]):
            outf.write(f'{accts_to_unit_id[orig_poster]},{k},1,{repost_period},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
            tot_ab_fol += v['ab_fol']
            tot_non_fol += v['non_fol']

    for control in list(control_accts):
        # avoid writing duplicate data
        if accts_to_unit_id[control] in seen_units:
            continue

        # get follow accumulation to control account
        follows_to_control = df_follows.filter(
            (pl.col('created_at') <= REPOST_CUTOFF) & \
            (pl.col('created_at') >= min_repost_day) & \
            (pl.col('to') == control)
        )
        follows_to_control = follows_to_control.join(
            followers_of_ab, 
            on='from', 
            how='left',
            suffix='_from_ab'
        )
        # figure out which follows to the control acct came from followers of the attention broker (AB)
        follows_to_control = follows_to_control.with_columns(
            pl.col('created_at_from_ab').fill_null(pl.lit(min_repost_day, dtype=Datetime).dt.replace_time_zone("UTC").cast(pl.Datetime("ms", "UTC")) + dt.timedelta(days=10 * 365))
        )
        follows_to_control = follows_to_control.with_columns(
            pl.col('created_at_from_ab').sub(
                pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_ab'),
            pl.col('created_at').sub(
                pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * HRS_PERIOD).alias('periods_until_followed_control')
        )
        # check if AB followers followed AB before they followed the control acct; if so, they count as followers of the AB
        follows_to_control = follows_to_control.with_columns(
            (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_control')).alias('followed_ab_before_control'),
        )
        
        # count AB follower + AB non-follower follows to the control acct
        per_day_ab_follower_follows_to_control = follows_to_control.filter(
            pl.col('followed_ab_before_control') == 1
        ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
            by=pl.col('periods_until_followed_control'))
        per_day_non_ab_follower_follows_to_control = follows_to_control.filter(
            pl.col('followed_ab_before_control') == 0
        ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
            by=pl.col('periods_until_followed_control'))

        # get the periods elapsed since the beginning of our earliest repost for this attention broker candidate.
        tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (HRS_PERIOD * 60 * 60))
        follows_per_period = {d: {'ab_fol': 0, 'non_fol': 0} for d in range(tot_periods + 1)}
        for df, label in [
            (per_day_ab_follower_follows_to_control, 'ab_fol'), 
            (per_day_non_ab_follower_follows_to_control, 'non_fol')
        ]:
            for row in df.iter_rows(named=True):
                follows_per_period[row['periods_until_followed_control']][label] = row['new_follows_per_period']
        
        # write per-period follower accumulation figures for followers and non-followers.
        tot_ab_fol = 0
        tot_non_fol = 0
        for k, v in sorted(follows_per_period.items(), key=lambda b: b[0]):
            outf.write(f'{accts_to_unit_id[control]},{k},0,{10000},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
            tot_ab_fol += v['ab_fol']
            tot_non_fol += v['non_fol']

# record that we've finished collating data for this account.
with open(f'{FILEPATH_OUT}/attention-brokers-bsky/finished_anon_ids.txt', 'a') as f_fin:
    f_fin.write(str(ab_ix) + '\n')

