import datetime as dt
import json
from zoneinfo import ZoneInfo

FILEPATH = '/Users/a404/attention-brokers-bsky' # change this for your machine
FILEPATH_OUT = FILEPATH # change this for your machine

# set conservative upper bound on repost events we'll study.
REPOST_CUTOFF = dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))
