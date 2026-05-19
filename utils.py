import datetime as dt 
import json
from zoneinfo import ZoneInfo

import polars as pl
from polars.datatypes import String, Int64, Datetime

"""
Utility functions for data parsing
"""

def get_followed_accts_and_unit_ids(df_follows, handle, ab_did, df_reposts):
    # get all the attention broker's followers
    followers_of_ab = df_follows.filter(pl.col('to') == ab_did)
    followed_by_ab = df_follows.filter(pl.col('from') == ab_did)
    print(f'{handle} follows {len(followed_by_ab)} accounts.')

    reposted_accts = set(pl.Series(df_reposts.select('orig_poster')).to_list())
    followed_accts = set(pl.Series(followed_by_ab.select('to')).to_list())

    accts_to_unit_id = {acct: ix for ix, acct in enumerate(
        sorted(list(reposted_accts | followed_accts)))
    }

    return followers_of_ab, followed_by_ab, accts_to_unit_id

def extend_final_dataframe(
    data_final,
    followers_before_repost, 
    followers_after_repost,
    unit_id,
    repost_period
):
    """
    Given dataframes of per-day follow counts before and after the repost, 
    concatenate the results to the final dataframe-in-the-making. 

    Inputs:
      data_final: list of dicts that will eventually become a polars dataframe.
      followers_before_repost: polars dataframe with per-day follow counts for every combination of 
        (days before/after repost, attention broker follower/non-follower) for which we have at least one follow event
        this is just for all the follow events that occurred prior to the attention broker's repost of the original poster.
      followers_after_repost: polars dataframe with per-day follow counts for every combination of 
        (days before/after repost, attention broker follower/non-follower) for which we have at least one follow event
        this is just for all the follow events that occurred after the attention broker's repost of the original poster.
      unit_id: int; uniquely indicates which account is reposted/being observed.
      repost_period: int; indicates how many days after the minimum repost event this repost occurred.

    Returns:
      data_final: list of dicts that will eventually become a polars dataframe. has the following columns:
        gain_rate: int; number of follows that occurred on that day for either followers or non-followers
        ever_treated: were these follows from followers or non-followers of the attention broker? 0 indicates non-followers; 1 indicates followers.
        unit_id: unique int identifier for the reposted or focal account.
        time_period: int; number of days that have passed since the earliest repost in the dataset.
        ts: int; days relative to the repost.
    
    """
    for df_fol in [followers_before_repost, followers_after_repost]:
        for row in df_fol.iter_rows(named=True):
            data_final.append({
                'gain_rate': row['from'],
                'ever_treated': row['ab_follower'],
                'unit_id': unit_id,
                'time_period': repost_period + row['days_before_after_repost'],
                'ts': row['days_before_after_repost'],
            })
    
    return data_final

def delineate_and_count_attention_broker_followers(followers, before=True):
    """
    Determine who "counts" as a follower of the attention broker; count follow events to reposted/focal account
    by followers and non-followers per day relative to the repost.

    Inputs:
      followers: polars dataframe with relevant columns as follows:
        from/index: the account doing the following
        created_at_from_ab: the datetime at which the account doing the following followed the attention broker, if applicable (can be NaN/empty)
        created_at: when the account doing the following followed the reposted/focal account (should never be empty/NaN)
        to: should always be the original poster/focal account
        repost_created_at: datetime; indicates when the repost occurred
      before: Boolean; True if followers contains follows that all occurred prior to the repost 
        and False if followers contains follows that all occurred after the repost.
    
    Outputs:
      followers: polars dataframe with per-day follow counts for every combination of 
      (days before/after repost, attention broker follower/non-follower) for which we have at least one follow event.
    """
    # figure out who is a follower of the attention broker and was therefore "treated" at the time they followed OP
    if before:
        # if a user followed the reposted/focal account prior to the repost, we count them as a follower of the attention broker 
        # if they followed the attention broker prior to following the reposted/focal account.
        followers = followers.with_columns(
            ((pl.col('created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
        )
    else:
        # if a user followed the reposted/focal account after the repost, we want to make sure that they followed the attention broker
        # prior to the attention broker's repost, so they theoretically could have been exposed to the repost.
        # this is perhaps a more conservative way of going about determining who "counts" as a follower.
        followers = followers.with_columns(
            ((pl.col('repost_created_at').sub(pl.col('created_at_from_ab'))).dt.total_seconds() > 0).alias('ab_follower')
        )

    # count per-day follows by followers and non-followers of the attention broker to the reposted account.
    followers = followers.group_by(
        [pl.col('days_before_after_repost'), pl.col('ab_follower')]).agg(pl.col('from').count())

    return followers

def partition_follows_before_after_repost(follows_to_op_following_ab, repost_created_at):
    """
    Partition follow events to the original poster/focal account into events that occurred either before or after the repost.

    Inputs:
      follows_to_op_following_ab: polars dataframe with relevant columns as follows:
        from/index: the account doing the following
        created_at_from_ab: the datetime at which the account doing the following followed the attention broker, if applicable (can be NaN/empty)
        created_at: when the account doing the following followed the reposted/focal account (should never be empty/NaN)
        to: should always be the original poster/focal account
        repost_created_at: datetime; indicates when the repost occurred
      repost_created_at: polars dataframe with one entry indicating when the repost occurred.

    Outputs:
      followers_before_repost: polars dataframe; contains all follow events that happened prior to the repost. 
      followers_after_repost: polars dataframe; contains all follow events that happened after the repost. 

      Note:
        followers_{before, after}_repost has a column days_before_after_repost indicating how many days (integer)
        before or after the repost the follow event occurred on. 12 hours after repost --> day 0. 12 hours before repost --> day 1. 
        36 hours after repost --> day 2. 

        We also fill null values in created_at_from_ab, which indicates when the following account (the index/"from" column) 
        followed the attention broker, with a date 5 years after the repost occurred. This makes it impossible for any accounts
        that had never followed the attention broker to be counted as followers of the attention broker.
    """
    # first, figure out when the follower --> reposted tie happened relative to the repost
    # pl.col('whatever1').sub(pl.col('whatever2')) subtracts the values in whatever2 from the values in whatever1.
    follows_to_op_following_ab = follows_to_op_following_ab.with_columns(
        ((pl.col('created_at').sub(pl.col('repost_created_at'))).dt.total_minutes().floordiv(24 * 60)).alias('days_before_after_repost'),
        (pl.col('created_at_from_ab').fill_null(repost_created_at.item() + dt.timedelta(days=5 * 365))),
    )
    # obtain all follow events prior to repost
    followers_before_repost = follows_to_op_following_ab.filter(
        pl.col('days_before_after_repost') < 0
    )
    # obtain all follow events after repost
    followers_after_repost = follows_to_op_following_ab.filter(
        pl.col('days_before_after_repost') >= 0
    )

    return followers_before_repost, followers_after_repost

def get_follows_to_reposted_account(
    df_follows, 
    orig_poster, 
    followers_of_ab, 
    repost_created_at, 
    high_follow_bound, 
    low_follow_bound,
):
    """
    Get all the follows to OP (or another focal account) that could've happened between low_follow_bound and high_follow_bound.

    Inputs:
      df_follows: polars dataframe; all follow events on Bluesky with timestamps
      orig_poster: string; Bluesky DID of the original poster being reposted (or other focal account, like a control account)
      followers_of_ab: polars dataframe; contains all follow events that are directed at the attention broker
      repost_created_at: polars dataframe; contains only the datetime object indicating when the repost by the attention broker happened.
      high_follow_bound: polars datetime; indicates the upper limit (temporally) on following events to orig_poster that we collect.
      low_follow_bound: polars datetime; indicates the lower limit (temporally) on following events to orig_poster that we collect. 

    Returns:
      follows_to_op_following_ab: polars dataframe. has the following relevant columns:
        from/index: the account doing the following
        created_at_from_ab: the datetime at which the account doing the following followed the attention broker, if applicable (can be NaN/empty)
        created_at: when the account doing the following followed the reposted/focal account (should never be empty/NaN)
        to: should always be the original poster/focal account
        repost_created_at: datetime; indicates when the repost occurred
    """
    # get all the follows to OP that could've happened in the time we observed
    follows_to_op = df_follows.filter(
        (pl.col('created_at') <= high_follow_bound) & \
        (pl.col('created_at') >= low_follow_bound) & \
        (pl.col('to') == orig_poster)
    )
    # populate with a column for when the repost was created
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
    # created_at is the time the follower --> reposted acct tie formed

    return follows_to_op_following_ab

def make_repost_df(
    filepath_to_reposts,
    handle, 
    ab_did, 
    repost_cutoff=dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC")),
):
    """
    Make a polars dataframe of an attention broker's reposts.

    Inputs:
      filepath_to_reposts: str; absolute file path to a directory containing a subdirectory named bsky_reposts. 
        bsky_reposts should contain JSON dicts of reposts labeled by the reposter's Bluesky handle.
      handle: str; Bluesky handle of the attention broker
      ab_did: str; Bluesky DID of the attention broker

    Returns:
      df_reposts: polars dataframe of reposts, with columns for repost timestamps and original poster DIDs
      min_repost_day: datetime; the earliest day on which we see a repost occur by this attention broker
      tot_reposts: int; total number of unique reposted accounts 
        (if there are multiple reposts of the same account, we take the first one only). 
    """
    # load attention broker's reposts and create a polars dataframe
    reposts = json.load(open(f'{filepath_to_reposts}/bsky_reposts/{handle}.json', 'r'))
    reposts = [parse_repost_dict(r) for r in reposts]
    df_reposts = pl.DataFrame(reposts)
    df_reposts = df_reposts.with_columns(
        pl.col('created_at').str.to_datetime(
            format='%Y-%m-%dT%H:%M:%S%.3f%Z', 
            time_zone='UTC'
        )
    )
    # filter to reposts that are before the cutoff date
    df_reposts = df_reposts.filter(pl.col('created_at') <= repost_cutoff)
    # filter out self-reposts
    df_reposts = df_reposts.filter(pl.col('orig_poster') != ab_did)
    # only analyze the first repost by the attention broker
    df_reposts = df_reposts.group_by(pl.col('orig_poster')).agg(pl.col('created_at').min())
    tot_reposts = len(df_reposts)
    
    # get earliest repost to make relative time_period column
    min_repost_day = df_reposts.select(pl.col('created_at')).min().item()
    print(min_repost_day)

    return df_reposts, min_repost_day, tot_reposts


def load_df_follows(filepath_to_follows, testing=False):
    """
    Given a folder where the follow graph lives, return a Polars dataframe of all follow events.

    This has columns "from", "to", and "created_at".
    It takes a while to run (on the order of an hour or two) because it's 220 GB of data.

    Input:
      filepath_to_follows: str; absolute path to a folder containing the CSV of follow events.
      testing: Boolean; defaults to False; indicates whether we should use the full follow file
        or follows_small.csv, which is around 2 GB of follow events.

    Returns: df_follows, a Polars dataframe.
    """
    if testing:
        follows_fname = 'follows_sample.csv'
    else:
        follows_fname = 'follows_all.csv'
    df_follows = pl.read_csv(
        f'{filepath_to_follows}/{follows_fname}', 
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

    return df_follows

def extract_did_from_uri(uri):
    """
    Extracts DID from a post URI (a DID is an account's distributed identifier)

    Input:
        uri: has format at://did:plc:SOME_TEXT/app.bsky.feed.post/SOME_TEXT

    Output:
        We extract the did:plc:SOME_TEXT portion of the URI.
    """
    uri_split_slashes = uri.split('/')
    return uri_split_slashes[2]

def parse_repost_dict(repost_dict):
    """
    Extract reposter and original posters' DIDs from raw JSON data;
    fix timezone on repost timestamp.

    Input:
        repost_dict: dict with keys 'uri', 'reposted', and 'created-at'. 
            uri: str; the URI of the repost
            reposted: the URI of the reposted post (i.e. original content)
            created-at: string datetime in %Y-%m-%dT%H:%M:%S%.3fZ format. 
                Z = UTC, so we replace Z with the offset for UTC.

    Output:
        dict of
            reposter DID, 
            original poster's DID,
            created_at string timestamp in %Y-%m-%dT%H:%M:%S%.3f%Z format.
    """
    reposter = extract_did_from_uri(repost_dict['uri'])
    orig_poster = extract_did_from_uri(repost_dict['reposted'])
    created_at = repost_dict['created-at']
    
    return {
        'reposter': reposter,
        'orig_poster': orig_poster,
        'created_at': created_at.replace('Z', '+00:00'),
    }