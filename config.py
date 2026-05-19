import json

FILEPATH = '/scratch/nte5cp' # change this for your machine
FILEPATH_OUT = '/home/nte5cp'

# set conservative upper bound on repost events we'll study.
REPOST_CUTOFF = dt.datetime(year=2025, month=9, day=15, tzinfo=ZoneInfo("UTC"))

AB_DIDS = json.load(open(f'{FILEPATH}/handles_to_dids.json', 'r'))