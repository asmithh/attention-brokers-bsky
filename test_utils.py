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
        'created_at': '2025-08-02T00:27:18.199+00:00',
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
    

class TestMakeRepostDF:
    def setup_class(self):
        self.ab_did = f'did:plc:{'a' * 24}'
        self.df_reposts, self.min_repost_day, self.tot_reposts = make_repost_df('./test_data', 'a', self.ab_did)
        self.reposts_raw = json.load(open('./test_data/bsky_reposts/a.json'))
        self.repost_cutoff=dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

    def test_repost_count_correct(self):
        # discard self-reposts and duplicate reposts of same non-self account
        # make sure repost count is correct
        assert self.tot_reposts == 2 

    def test_min_repost_day_correct(self):
        # make sure min repost day is the minimum timestamp of a non-self-repost
        assert self.min_repost_day.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' == \
            min([d['created-at'] for d in self.reposts_raw if self.ab_did not in d['reposted']])
    
    def test_no_reposts_after_cutoff(self):
        # make sure no reposts appear after the cutoff
        assert self.df_reposts.select(pl.max('created_at')).item() < self.repost_cutoff

    def test_no_self_reposts(self):
        # make sure no self-reposts remain
        assert self.ab_did not in set(pl.Series(self.df_reposts.select('orig_poster')).to_list())

    def test_only_first_multiple_repost_used(self):
        # make sure only the first repost of multiply reposted accounts remains
        for reposted in pl.Series(self.df_reposts.select('orig_poster')).to_list():
            reposted_on = self.df_reposts.filter(pl.col('orig_poster') == reposted).select('created_at').item()
            assert reposted_on.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' == \
                min([d['created-at'] for d in self.reposts_raw if reposted in d['reposted']])
        
class TestGetFollowedAcctsAndUnitIDs:
    def setup_class(self):
        self.ab_did = f'did:plc:{'a' * 24}'
        self.handle = 'a'
        self.follows_raw = pl.read_csv(
            './test_data/follows_sample.csv', 
            has_header=False, 
            new_columns=["from", "to", "created_at"],
        )
        self.follows_raw = self.follows_raw.filter(~pl.col('created_at').str.ends_with('+07:00'))
        self.raw_ab_followers = set(pl.Series(self.follows_raw.filter(pl.col('to') == self.ab_did).select('from')).to_list())
        self.raw_followed_by_ab = set(pl.Series(self.follows_raw.filter(pl.col('from') == self.ab_did).select('to')).to_list())

        self.df_reposts, _, _ = make_repost_df('./test_data', self.handle, self.ab_did)
        self.df_follows = load_df_follows('./test_data', testing=True)
        self.followers_of_ab, self.followed_by_ab, self.accts_to_unit_id = get_followed_accts_and_unit_ids(
            self.df_follows, self.handle, self.ab_did, self.df_reposts)
    
    def test_ab_followers_correct(self):
        assert self.raw_ab_followers == set(pl.Series(self.followers_of_ab.select('from')).to_list())
    
    def test_followed_by_ab_correct(self):
        assert self.raw_followed_by_ab == set(pl.Series(self.followed_by_ab.select('to')).to_list())

    def test_correct_number_of_units(self):
        assert len(self.accts_to_unit_id) == len(self.df_reposts) + len(self.raw_followed_by_ab)

class TestGetFollowsToRepostedAccount:
    def setup_class(self):
        self.orig_poster = f'did:plc:{'ab' * 12}'
        self.handle = 'a'
        self.ab_did = f'did:plc:{'a' * 24}'

        self.df_reposts, _, _ = make_repost_df('./test_data', self.handle, self.ab_did)
        self.df_follows = load_df_follows('./test_data', testing=True)
        self.followers_of_ab, _, _ = get_followed_accts_and_unit_ids(
            self.df_follows, 
            self.handle, 
            self.ab_did, 
            self.df_reposts
        )

        self.individual_repost = [i for i in self.df_reposts.filter(pl.col('orig_poster') == self.orig_poster).iter_rows(named=True)][0]
        self.repost_created_at = pl.DataFrame({'created_at': [self.individual_repost['created_at']]})
        self.high_follow_bound = self.individual_repost['created_at'] + dt.timedelta(days=14)
        self.low_follow_bound = self.individual_repost['created_at'] - dt.timedelta(days=14)
        self.follows_to_op_following_ab = get_follows_to_reposted_account(
            self.df_follows, 
            self.orig_poster, 
            self.followers_of_ab, 
            self.repost_created_at, 
            self.high_follow_bound, 
            self.low_follow_bound,
        )
        self.followed_op_ever = self.df_follows.filter(pl.col('to') == self.orig_poster)
        self.followed_op_in_period = set(pl.Series(
            self.followed_op_ever.filter(
                (pl.col('created_at') <= self.high_follow_bound) & \
                (pl.col('created_at') >= self.low_follow_bound)).select('from')
        ).to_list())

    def test_follows_to_op_non_null(self):
        # make sure all follows to OP have non-null timestamps
        assert self.follows_to_op_following_ab.select(pl.col("created_at").has_nulls()).item() == False
    
    def test_follows_to_op_within_range(self):
        # make sure all follows to OP are within time bounds given 
        min_follow_to_op = self.follows_to_op_following_ab.select(pl.min("created_at")).item()
        max_follow_to_op = self.follows_to_op_following_ab.select(pl.max("created_at")).item()
        assert min_follow_to_op >= self.low_follow_bound
        assert max_follow_to_op <= self.high_follow_bound

    def test_repost_creation_timestamp(self):
        # make sure repost creation timestamp column is correct & only takes one value
        assert self.repost_created_at.item() == self.follows_to_op_following_ab.select(pl.col("repost_created_at")).unique().item()

    def test_followers_to_op_correct(self):
        # make sure the set of people who followed OP during the time period is correct
        follows_to_op_from_function = set(pl.Series(self.follows_to_op_following_ab.select(pl.col('from'))).to_list())
        assert self.followed_op_in_period == follows_to_op_from_function

    def test_op_and_ab_follower_sets_correct(self):
        # make sure the set of people who followed OP during the time period intersected with the set of people who follow the attention broker 
        # is of the same cardinality as the rows of the dataframe where created_at_from_ab is not null.
        set_ab_followers = set(pl.Series(self.followers_of_ab.select('from')))

        assert self.followed_op_in_period & set_ab_followers == set(pl.Series(
            self.follows_to_op_following_ab.filter(~pl.col('created_at_from_ab').is_null()).select('from')
        ).to_list())

        # make sure the number of null values is also correct.
        assert len(self.followed_op_in_period - set_ab_followers) == len(self.follows_to_op_following_ab.filter(pl.col('created_at_from_ab').is_null()))

    def test_follow_timestamps_to_abs_correct(self):
        # make sure the following event timestamps for the follows to the attention brokers are correct.
        for follow in self.follows_to_op_following_ab.filter(~pl.col('created_at_from_ab').is_null()).iter_rows(named=True):
            assert follow['created_at'] == self.df_follows.filter(
                (pl.col('from') == follow['from']) & \
                (pl.col('to') == follow['to'])
            ).select('created_at').item()

class TestPartitionFollowsBeforeAfterRepost:
    def setup_class(self):
        self.orig_poster = f'did:plc:{'ab' * 12}'
        self.handle = 'a'
        self.ab_did = f'did:plc:{'a' * 24}'

        self.df_reposts, _, _ = make_repost_df('./test_data', self.handle, self.ab_did)
        self.df_follows = load_df_follows('./test_data', testing=True)
        self.followers_of_ab, _, _ = get_followed_accts_and_unit_ids(
            self.df_follows, 
            self.handle, 
            self.ab_did, 
            self.df_reposts
        )

        self.individual_repost = [i for i in self.df_reposts.filter(pl.col('orig_poster') == self.orig_poster).iter_rows(named=True)][0]
        self.repost_created_at = pl.DataFrame({'created_at': [self.individual_repost['created_at']]})
        self.high_follow_bound = self.individual_repost['created_at'] + dt.timedelta(days=14)
        self.low_follow_bound = self.individual_repost['created_at'] - dt.timedelta(days=14)
        self.follows_to_op_following_ab = get_follows_to_reposted_account(
            self.df_follows, 
            self.orig_poster, 
            self.followers_of_ab, 
            self.repost_created_at, 
            self.high_follow_bound, 
            self.low_follow_bound,
        )

        self.follows_before_repost, self.follows_after_repost = partition_follows_before_after_repost(
            self.follows_to_op_following_ab, 
            self.repost_created_at
        )

    def test_follows_before_repost_accurate(self):
        # check that follows_before_repost contains only accounts that followed the OP between low_follow_bound and the repost.
        assert len(self.follows_before_repost) == 10 # should be 5 accounts following OP between day -1 and day 0, plus 5 between day -14 and day -1.
        # check time bounds on follow events.
        assert self.follows_before_repost.select(pl.col('created_at').min()).item() >= self.low_follow_bound
        assert self.follows_before_repost.select(pl.col('created_at').max()).item() < self.repost_created_at.item()

    def test_follows_after_repost_accurate(self):
        # check that follows_after_repost contains only accounts that followed the OP between the repost and high_follow_bound.
        assert len(self.follows_after_repost) == 15 # should be 10 accounts following OP between day 0 and day 1, plus 5 between day 1 and day 14.
        # check time bounds on follow events.
        assert self.follows_after_repost.select(pl.col('created_at').min()).item() >= self.repost_created_at.item()
        assert self.follows_after_repost.select(pl.col('created_at').max()).item() < self.high_follow_bound

    def test_null_ab_follows_filled_correctly(self):
        # make sure null values in created_at_from_ab are filled in with the correct value
        assert self.follows_after_repost.select(pl.col('created_at_from_ab').is_null().sum()).item() == 0
        assert self.follows_before_repost.select(pl.col('created_at_from_ab').is_null().sum()).item() == 0
        assert self.follows_before_repost.select(pl.col('created_at_from_ab').max()).item() == self.repost_created_at.item() + dt.timedelta(days=5 * 365)
        assert self.follows_after_repost.select(pl.col('created_at_from_ab').max()).item() == self.repost_created_at.item() + dt.timedelta(days=5 * 365)

    def test_days_before_after_repost_correct(self):
        # since this is an area where mishaps can occur,
        # make sure that the follows between [-1, 0) and [0, 1) are correctly delineated.
        follows_day_minus_1 = set(pl.Series(self.follows_before_repost.filter(
            pl.col('days_before_after_repost') == -1
        ).select(pl.col('from'))))
        follows_day_zero = set(pl.Series(self.follows_after_repost.filter(
            pl.col('days_before_after_repost') == 0
        ).select(pl.col('from'))))
        day_zero_upper_bound = self.individual_repost['created_at'] + dt.timedelta(days=1)
        day_minus_one_lower_bound = self.individual_repost['created_at'] - dt.timedelta(days=1)

        follows_to_op = self.df_follows.filter(pl.col('to') == self.orig_poster)
        ground_truth_day_minus_1 = set(pl.Series(
            follows_to_op.filter(
                (pl.col('created_at') >= day_minus_one_lower_bound) & \
                (pl.col('created_at') < self.repost_created_at.item())
            ).select('from')).to_list())
        assert ground_truth_day_minus_1 == follows_day_minus_1

        ground_truth_day_zero = set(pl.Series(
            follows_to_op.filter(
                (pl.col('created_at') >= self.repost_created_at.item()) & \
                (pl.col('created_at') < day_zero_upper_bound)
            ).select('from')).to_list())
        assert ground_truth_day_zero == follows_day_zero

class TestDelineateAndCountAttentionBrokerFollowers:
    def setup_class(self):
        self.orig_poster = f'did:plc:{'ab' * 12}'
        self.handle = 'a'
        self.ab_did = f'did:plc:{'a' * 24}'

        self.df_reposts, _, _ = make_repost_df('./test_data', self.handle, self.ab_did)
        self.df_follows = load_df_follows('./test_data', testing=True)
        self.followers_of_ab, _, _ = get_followed_accts_and_unit_ids(
            self.df_follows, 
            self.handle, 
            self.ab_did, 
            self.df_reposts
        )

        self.individual_repost = [i for i in self.df_reposts.filter(pl.col('orig_poster') == self.orig_poster).iter_rows(named=True)][0]
        self.repost_created_at = pl.DataFrame({'created_at': [self.individual_repost['created_at']]})
        self.high_follow_bound = self.individual_repost['created_at'] + dt.timedelta(days=14)
        self.low_follow_bound = self.individual_repost['created_at'] - dt.timedelta(days=14)
        self.follows_to_op_following_ab = get_follows_to_reposted_account(
            self.df_follows, 
            self.orig_poster, 
            self.followers_of_ab, 
            self.repost_created_at, 
            self.high_follow_bound, 
            self.low_follow_bound,
        )