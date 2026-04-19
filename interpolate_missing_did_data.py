import sys

import pandas as pd
import numpy as np

"""
Used to interpolate missing data points in the CSVs that will be used to do the differences-in-differences analysis.
The raw data has missing rows on for [unit_id, ts, ever_treated] combinations where no follows were accumulated to the reposted account.
This code fills in the missing rows with zeros for gain_rate and interpolated time_period based on alignment between 
existing time_period values and ts values. 

Run as python3 interpolate_missing_did_data.py $HANDLE $DAYS_FWD $DAYS_BWD
HANDLE is the Bluesky handle of the attention broker
DAYS_FWD is the number of days for which we have data after the repost
DAYS_BWD is the number of days for which we have data before the repost
"""

HANDLE = sys.argv[1]
DAYS_FWD = int(sys.argv[2])
DAYS_BWD = int(sys.argv[3])
FTYPE = sys.argv[4]

if FTYPE == 'did':
    ORIG_CSV_DIR = 'did_csvs'
    OUT_CSV_DIR = 'interpolated_did_csvs'
elif FTYPE == 'control':
    ORIG_CSV_DIR = 'control_csvs'
    OUT_CSV_DIR = 'interpolated_control_csvs'

FILEPATH = '/home/nte5cp' # change this for your machine

df = pd.read_csv(f'{FILEPATH}/{ORIG_CSV_DIR}/{HANDLE}_fwd_{DAYS_FWD}_bwd_{DAYS_BWD}.csv')

# Building a MultiIndex to fill in NaNs for missing data.
# These are the combinations of values we should have, but some combinations will be missing.
iterables = [df['unit_id'].unique(), [True, False], range(-1 * DAYS_BWD, DAYS_FWD)]
# Create a new index and reset index
# per this StackOverflow post:
# https://stackoverflow.com/questions/25909984/missing-data-insert-rows-in-pandas-and-fill-with-nan
idx = pd.MultiIndex.from_product(iterables, names=["unit_id", "ever_treated", "ts"]) 
df.set_index(["unit_id", "ever_treated", "ts"], inplace=True)
df = df.reindex(idx)
df = df.sort_index() # sort index to make manipulations on time_period easier
df = df.reset_index() # reset index to be a single index, not a MultiIndex
df['gain_rate'] = df['gain_rate'].fillna(0) # fill in zero gain_rate values.

def complete_interpolation_for_unit(gr):
    """
    Given a chunk of a dataframe for one unit (i.e. reposted account),
    fully recreate the time_period column by interpolating based on alignment
    between the ts column and existing values in the time_period column.

    Input:
        gr: dataframe; one group from a pandas GroupBy. must have columns 'ts' and 'time_period'.

    Output:
        an array of time_period values; the values for non-followers are concatenated to the values for followers:
        [NON_FOLLOWER_TIME_PERIOD_VALUES] + [FOLLOWER_TIME_PERIOD_VALUES]
    """
    def interpolate_arrays(row):
        """
        Given row, a chunk of a dataframe (subset of gr in the main function), return either an empty list 
        if interpolation is impossible or the values for time_period informed by any alignment between the 
        ts column and the time_period column.

        Input:
            row: dataframe; either the subset of gr where ever_treated is True or False. Must have columns 'ts' and 'time_period'.
            
        Output:
            if 'time_period' is all NaN values (i.e. no follows were observed for this group/unit_id combo), then return an empty list.
            else, we find the offset between ts and time_period and use that to recreate time_period in full.
            this will return a list of length 28, aligned with the [-14, 14) of ts. 
        """
        ts = row['ts']
        time_period = row['time_period']
        # find offset between ts and time_period.
        for ts_rel, ts_per in zip(ts, time_period):
            if not(np.isnan(ts_per)):
                offset = ts_per - ts_rel

        try:
            # interpolate based on offset
            return [ts_rel + offset if np.isnan(ts_per) else ts_per for ts_rel, ts_per in zip(ts, time_period)]
        except Exception as e:
            # if offset is unknown, return an empty list.
            return []

    # create time_period results for both values of ever_treated.
    # if a unit_id is in our original dataframe, we observed at least one follow to unit_id, so we have
    # enough information to make the alignment and, if needed, extrapolate it to the other value of ever_treated.
    periods = {True: [], False: []}
    for treat in [True, False]:
        gr_sub = gr[gr['ever_treated'] == treat]
        periods[treat] = interpolate_arrays(gr_sub)
    if periods[True] == []:
        periods[True] = periods[False]
    elif periods[False] == []:
        periods[False] = periods[True]
    return periods[False] + periods[True] # concatenate in correct order to apply to sorted dataframe.

time_period_by_unit = df.groupby('unit_id').apply(complete_interpolation_for_unit, include_groups=False).explode() # flatten list of lists
df['time_period'] = time_period_by_unit.to_list() # add interpolated time_period column

df.to_csv(f'{FILEPATH}/{OUT_CSV_DIR}/{HANDLE}_fwd_{DAYS_FWD}_bwd_{DAYS_BWD}.csv')
