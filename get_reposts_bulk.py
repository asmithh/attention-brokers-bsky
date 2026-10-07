import requests
import json
import os
import time

import pandas as pd

from config import *

def get_all_reposts(did, fout):
    """
    Given a Bluesky DID and a .json filename, 
    write all reposts by that DID to the .json filename as a list of dicts.

    Inputs:
      handle: string; valid Bluesky DID
      fout: string; valid .json filename
    
    Outputs:
      none; writes a JSON object (list of dicts) to fout.
    """
    if os.path.exists(fout):
        return True
    has_more = True
    cursor = ""
    all_reposts = []
    fail_count = 0
    while has_more:
        batch = requests.get(
             "https://bsky.social/xrpc/com.atproto.repo.listRecords",
              params={
                 "repo": did,
               "collection": "app.bsky.feed.repost",
                 "cursor": cursor,
                  "limit": 100,
              },
         )
        try:
            print("I am inside a try/except")
            batch = batch.json()
            all_reposts.extend(batch['records'])
        except Exception as e:
            print(e)
            fail_count += 1
            if fail_count > 7:
                has_more = False
                print(f'error with {did}!')
            else:
                print(batch, flush=True)
                time.sleep(2 ** fail_count)
        if 'cursor' in batch:
            cursor = batch['cursor']
        else:
            has_more = False
    
    reposts = [
        {
        'uri': r['uri'],
        'created-at': r["value"]["createdAt"],
        "reposted": r["value"]["subject"]["uri"],
        "raw": r["value"],
        }
        for r in all_reposts
    ]
    with open(fout, 'w') as f:
        json.dump(reposts, f)
        
    return False

df_cand = pd.read_csv('candidates_with_dids_by_20250915.csv')

for ix, data in df_cand.iterrows():
    did = data['did']
    anon_id = data['src_poster']
    fout = f'{FILEPATH_OUT}/attention-brokers-bsky/bsky_reposts_new_candidates/{anon_id}.json'
    res = get_all_reposts(did, fout)
    print(anon_id, flush=True)
    if not(res):
        time.sleep(60)