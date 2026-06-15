import datetime as dt 
import json
from zoneinfo import ZoneInfo

import polars as pl
from polars.datatypes import Datetime

"""
Utility functions for data parsing
"""

def get_followed_accts_and_unit_ids_with_delineation(df_follows, handle, ab_did, df_reposts, reposted_before):
    """
    Get the set of accounts that the attention broker follows; map DIDs to unit IDs; 
    delineate control and treated accounts to analyze.

    Inputs:
      df_follows: polars dataframe of follow events w/ columns:
        from: DID of the user doing the following
        to: DID of the user being followed
        created-at: datetime at which the follow event occurred.
      handle: str; attention broker's handle
      ab_did: str; attention broker's DID
      df_reposts: polars dataframe of repost events; prefiltered to be in the desired time period
      reposted_before: set of DIDs of accounts that were reposted before the desired time period

    Returns:
      followers_of_ab: polars dataframe containing accounts that follow the AB & when the follow event occurred
      followed_by_ab: polars dataframe containing accounts that are followed by the AB & when the follow event occurred
      reposted_accounts_followed_by_ab: set of reposted accounts (reposted in the desired time period) that the AB follows
      control_accts: set of accounts that are followed by the AB but never reposted
      accts_to_unit_id: maps DIDs to unique/anonymous integer IDs.
    """
    # get all the attention broker's followers and followees
    followers_of_ab = df_follows.filter(pl.col('to') == ab_did)
    followed_by_ab = df_follows.filter(pl.col('from') == ab_did)
    print(f'{handle} follows {len(followed_by_ab)} accounts.')

    # get accounts reposted in the desired time period
    reposted_accts = set(pl.Series(df_reposts.select('orig_poster')).to_list())
    # get followees of AB into a set
    followed_accts = set(pl.Series(followed_by_ab.select('to')).to_list())

    # filter reposted accounts to just the ones the AB follows
    reposted_accts_followed_by_ab = reposted_accts.intersection(followed_accts)
    # control accounts are the never-reposted accounts
    control_accts = followed_accts - reposted_accts
    control_accts = control_accts - reposted_before

    # make a mapping from DID to anonymous integer ID
    accts_to_unit_id = {acct: ix for ix, acct in enumerate(
        sorted(list(followed_accts)))
    }

    return followers_of_ab, followed_by_ab, reposted_accts_followed_by_ab, control_accts, accts_to_unit_id

def make_repost_df(
    filepath_to_reposts,
    handle, 
    ab_did, 
    repost_cutoff=dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC")),
    left_time_cutoff=dt.datetime(year=2025, month=1, day=1, tzinfo=ZoneInfo("UTC")),
):
    """
    Make a polars dataframe of an attention broker's reposts.

    Inputs:
      filepath_to_reposts: str; absolute file path to a directory containing a subdirectory named bsky_reposts. 
        bsky_reposts should contain JSON dicts of reposts labeled by the reposter's Bluesky handle.
      handle: str; Bluesky handle of the attention broker
      ab_did: str; Bluesky DID of the attention broker
      repost_cutoff: datetime; maximum day for which we want to observe reposts
      left_time_cutoff: datetime; minimum day for which we want to observe reposts

    Returns:
      df_reposts: polars dataframe of reposts, with columns for repost timestamps and original poster DIDs
      min_repost_day: datetime; the earliest day on which we see a repost occur by this attention broker
      tot_reposts: int; total number of unique reposted accounts 
        (if there are multiple reposts of the same account, we take the first one only). 
      reposted_before_left_cutoff: set; DIDs of accounts that were reposted before the minimum repost collection date
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
    # get ever-reposted accounts
    df_pre = df_reposts.filter(
        pl.col('created_at') < left_time_cutoff
    )
    reposted_before_left_cutoff = set(df_pre['orig_poster'].to_list())

    # filter to reposts that are between the cutoff dates
    df_reposts = df_reposts.filter(
        (pl.col('created_at') <= repost_cutoff) & \
        (pl.col('created_at') >= left_time_cutoff)
    )
    # filter out self-reposts
    df_reposts = df_reposts.filter(pl.col('orig_poster') != ab_did)

    # only analyze the first repost by the attention broker
    df_reposts = df_reposts.group_by(pl.col('orig_poster')).agg(
        pl.col('created_at').min())
    tot_reposts = len(df_reposts)
    
    # get earliest repost to make relative time_period column
    min_repost_day = df_reposts.select(pl.col('created_at')).min().item()
    print(min_repost_day)

    return df_reposts, min_repost_day, tot_reposts, reposted_before_left_cutoff


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

    df_follows = df_follows.drop_nulls() # drop null parsed datetimes (where we failed to parse)
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