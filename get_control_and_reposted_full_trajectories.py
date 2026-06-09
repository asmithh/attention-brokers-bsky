import datetime as dt
import sys

import polars as pl

from config import FILEPATH, FILEPATH_OUT, AB_DIDS, REPOST_CUTOFF
from utils import *

AB_HANDLES = []
with open(sys.argv[1], 'r') as f:
    for line in f.readlines():
        AB_HANDLES.append(line.strip())

for handle in AB_HANDLES:
    df_follows = load_df_follows(FILEPATH)
    ab_did = AB_DIDS[handle]

    df_reposts, min_repost_day, tot_reposts = make_repost_df(FILEPATH, handle, ab_did)
    followers_of_ab, followed_by_ab, reposted_accts_followed_by_ab, control_accts, accts_to_unit_id = get_followed_accts_and_unit_ids_with_delineation(df_follows, handle, ab_did, df_reposts)

    with open (f'{handle}_follows_to_ops_and_controls.csv', 'w') as outf:
        outf.write('unit_id,period,ever_treated,period_treated,tot_ab_fol,tot_non_fol\n')
        for row in df_reposts.iter_rows(named=True):
            orig_poster = row['orig_poster']
            if orig_poster not in accts_to_unit_id:
                continue
            repost_created_at = pl.DataFrame({'created_at': [row['created_at']]})
            repost_period = (repost_created_at.item() - min_repost_day).total_seconds() // (60 * 60 * 12)
            # op_followers_before_min_repost_day = df_follows.filter(
            #     (pl.col('created_at') < min_repost_day) & \
            #     (pl.col('to') == orig_poster)
            # )
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
            follows_to_op = follows_to_op.with_columns(
                pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))
            )
            follows_to_op = follows_to_op.with_columns(
                pl.col('created_at_from_ab').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * 12).alias('periods_until_followed_ab'),
                pl.col('created_at').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * 12).alias('periods_until_followed_op')
            )
            follows_to_op = follows_to_op.with_columns(
                (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_op')).alias('followed_ab_before_op'),
            )
            per_day_ab_follower_follows_to_op = follows_to_op.filter(
                pl.col('followed_ab_before_op') == 1
            ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_op'))
            per_day_non_ab_follower_follows_to_op = follows_to_op.filter(
                pl.col('followed_ab_before_op') == 0
            ).group_by(pl.col('periods_until_followed_op')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_op'))

            
            tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (12 * 60 * 60))
            follows_per_period = {d: {'ab_fol': 0, 'non_fol': 0} for d in range(tot_periods + 1)}
            for df, label in [
                (per_day_ab_follower_follows_to_op, 'ab_fol'), 
                (per_day_non_ab_follower_follows_to_op, 'non_fol')
            ]:
                for row in df.iter_rows(named=True):
                    follows_per_period[row['periods_until_followed_op']][label] = row['new_follows_per_period']
            
        
            tot_ab_fol = 0
            tot_non_fol = 0
            for k, v in sorted(follows_per_period.items(), key=lambda b: b[0]):
                outf.write(f'{accts_to_unit_id[orig_poster]},{k},1,{repost_period},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
                tot_ab_fol += v['ab_fol']
                tot_non_fol += v['non_fol']
        for control in list(control_accts):
            # op_followers_before_min_repost_day = df_follows.filter(
            #     (pl.col('created_at') < min_repost_day) & \
            #     (pl.col('to') == orig_poster)
            # )
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
            follows_to_control = follows_to_control.with_columns(
                pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))
            )
            follows_to_control = follows_to_control.with_columns(
                pl.col('created_at_from_ab').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * 12).alias('periods_until_followed_ab'),
                pl.col('created_at').sub(
                    pl.lit(min_repost_day, dtype=Datetime)).dt.total_minutes().floordiv(60 * 12).alias('periods_until_followed_control')
            )
            follows_to_control = follows_to_control.with_columns(
                (pl.col('periods_until_followed_ab') < pl.col('periods_until_followed_control')).alias('followed_ab_before_control'),
            )
            per_day_ab_follower_follows_to_control = follows_to_control.filter(
                pl.col('followed_ab_before_control') == 1
            ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_control'))
            per_day_non_ab_follower_follows_to_control = follows_to_control.filter(
                pl.col('followed_ab_before_control') == 0
            ).group_by(pl.col('periods_until_followed_control')).agg(pl.len().alias("new_follows_per_period")).sort(
                by=pl.col('periods_until_followed_control'))

            
            tot_periods = int((REPOST_CUTOFF - min_repost_day).total_seconds() // (12 * 60 * 60))
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
                outf.write(f'{accts_to_unit_id[control]},{k},0,{10000},{tot_ab_fol + v['ab_fol']},{tot_non_fol + v['non_fol']}\n')
                tot_ab_fol += v['ab_fol']
                tot_non_fol += v['non_fol']

    


