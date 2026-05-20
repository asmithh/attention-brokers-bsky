import json

import polars as pl
import pytest

from utils import *

def test_parse_repost_dict():
    repost_dicts = json.load(open('./test_data/bsky_reposts/a.json'))
    repost_dict = repost_dicts[3]
    parsed = parse_repost_dict(repost_dict)

    desired_res = {
        'reposter': 'did:plc:aaaaaaaaaaaaaaaaaaaaaaaa',
        'orig_poster': 'did:plc:abababababababababababab',
        'created_at': '2024-08-12T10:20:14.835+00:00',
    }
    assert parsed == desired_res

def test_extract_did_from_uri():
    did = 'a' * 24
    post_id = 'b' * 13
    uri1 = f'at://did:plc:{did}/app.bsky.feed.repost/{post_id}'
    assert extract_did_from_uri(uri1) == 'did:plc:' + did

    uri2 = 'abc123'
    with pytest.raises(IndexError):
        extract_did_from_uri(uri2) 

def test_load_df_follows():
    df_raw = pl.read_csv(
        './test_data/follows_sample.csv', 
        has_header=False, 
        new_columns=["from", "to", "created_at"],
    )
    df_fol = load_df_follows('./test_data', testing=True)

    bad_timestamps = len(df_raw.filter(pl.col('created_at').str.ends_with('+07:00')))
    # test if drops rows with null values and unparseable timestamps
    assert len(df_raw) == len(df_fol) + df_raw.null_count().pipe(sum).item() + bad_timestamps
    

def test_make_repost_df():
    ab_did = f'did:plc:{'a' * 24}'
    df_reposts, min_repost_day, tot_reposts = make_repost_df('./test_data', 'a', ab_did)
    reposts_raw = json.load(open('./test_data/bsky_reposts/a.json'))

    repost_cutoff=dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

    # discard self-reposts and duplicate reposts of same non-self account
    # make sure repost count is correct
    assert tot_reposts == 2 

    # make sure min repost day is the minimum timestamp of a non-self-repost
    assert min_repost_day.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' == \
          min([d['created-at'] for d in reposts_raw if ab_did not in d['reposted']])
    
    # make sure no reposts appear after the cutoff
    assert df_reposts.select(pl.max('created_at')).item() < repost_cutoff

    # make sure no self-reposts remain
    assert ab_did not in set(pl.Series(df_reposts.select('orig_poster')).to_list())

    # make sure only the first repost of multiply reposted accounts remains
    for reposted in pl.Series(df_reposts.select('orig_poster')).to_list():
        reposted_on = df_reposts.filter(pl.col('orig_poster') == reposted).select('created_at').item()
        assert reposted_on.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' == \
            min([d['created-at'] for d in reposts_raw if reposted in d['reposted']])
        
def test_get_followed_accts_and_unit_ids():
    ab_did = f'did:plc:{'a' * 24}'
    handle = 'a'
    follows_raw = pl.read_csv(
        './test_data/follows_sample.csv', 
        has_header=False, 
        new_columns=["from", "to", "created_at"],
    )
    follows_raw = follows_raw.filter(~pl.col('created_at').str.ends_with('+07:00'))
    raw_ab_followers = set(pl.Series(follows_raw.filter(pl.col('to') == ab_did).select('from')).to_list())
    raw_followed_by_ab = set(pl.Series(follows_raw.filter(pl.col('from') == ab_did).select('to')).to_list())

    df_reposts, _, _ = make_repost_df('./test_data', handle, ab_did)
    df_follows = load_df_follows('./test_data', testing=True)
    followers_of_ab, followed_by_ab, accts_to_unit_id = get_followed_accts_and_unit_ids(df_follows, handle, ab_did, df_reposts)
    
    assert raw_ab_followers == set(pl.Series(followers_of_ab.select('from')).to_list())
    assert raw_followed_by_ab == set(pl.Series(followed_by_ab.select('to')).to_list())

    assert len(accts_to_unit_id) == len(df_reposts) + len(raw_followed_by_ab)

def test_get_follows_to_reposted_account():
    orig_poster = f'did:plc:{'ab' * 12}'
    handle = 'a'
    ab_did = f'did:plc:{'a' * 24}'

    df_reposts, _, _ = make_repost_df('./test_data', handle, ab_did)
    df_follows = load_df_follows('./test_data', testing=True)
    followers_of_ab, _, _ = get_followed_accts_and_unit_ids(df_follows, handle, ab_did, df_reposts)
    print(followers_of_ab)
    print(df_follows.filter(pl.col('to') == orig_poster))
    individual_repost = [i for i in df_reposts.filter(pl.col('orig_poster') == orig_poster).iter_rows(named=True)][0]
    repost_created_at = pl.DataFrame({'created_at': [individual_repost['created_at']]})
    high_follow_bound = individual_repost['created_at'] + dt.timedelta(days=14)
    low_follow_bound = individual_repost['created_at'] - dt.timedelta(days=14)
    follows_to_op_following_ab = get_follows_to_reposted_account(
        df_follows, 
        orig_poster, 
        followers_of_ab, 
        repost_created_at, 
        high_follow_bound, 
        low_follow_bound,
    )

    # make sure all follows are within time bounds given
    # make sure repost creation timestamp column is correct
    # make sure the set of people who followed OP during the time period is correct
    # make sure the set of people who followed OP during the time period intersected with the set of people who follow the attention broker 
    # is of the same cardinality as the rows of the dataframe where created_at_from_ab is not null.
    # make sure the number of null values is also correct.
    # make sure the following event timestamps for the follows to the attention brokers are correct.
    print(follows_to_op_following_ab.filter(pl.col('created_at') > pl.col('created_at_from_ab')))
