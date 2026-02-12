import sys

import pandas as pd
import numpy as np

HANDLE = sys.argv[1]
FILEPATH = '/scratch/nte5cp'

df = pd.read_csv(f'{FILEPATH}/did_csvs/{HANDLE}.csv')

iterables = [df['unit_id'].unique(), [True, False], range(-14, 14)]
idx = pd.MultiIndex.from_product(iterables, names=["unit_id", "ever_treated", "ts"])
df.set_index(["unit_id", "ever_treated", "ts"], inplace=True)
df = df.reindex(idx)
df = df.sort_index()
df = df.reset_index()
df['gain_rate'] = df['gain_rate'].fillna(0)

def complete_interpolation_for_unit(gr):
    def interpolate_arrays(row):
        ts = row['ts']
        time_period = row['time_period']
        for ts_rel, ts_per in zip(ts, time_period):
            if not(np.isnan(ts_per)):
                offset = ts_per - ts_rel
        try:
            return [ts_rel + offset if np.isnan(ts_per) else ts_per for ts_rel, ts_per in zip(ts, time_period)]
        except Exception as e:
            return []
            
    periods = {True: [], False: []}
    for treat in [True, False]:
        gr_sub = gr[gr['ever_treated'] == treat]
        periods[treat] = interpolate_arrays(gr_sub)
    if periods[True] == []:
        periods[True] = periods[False]
    elif periods[False] == []:
        periods[False] = periods[True]
    return periods[False] + periods[True]

time_period_by_unit = df.groupby('unit_id').apply(complete_interpolation_for_unit).explode()
df['time_period'] = time_period_by_unit.to_list()

df.to_csv(f'{FILEPATH}/interpolated_did_csvs/{HANDLE}.csv')