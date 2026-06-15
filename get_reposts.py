import requests
import json

from config import *

def get_all_reposts(handle, fout):
    """
    Given a Bluesky handle and a .json filename, 
    write all reposts by the handle to the .json filename as a list of dicts.

    Inputs:
      handle: string; valid Bluesky handle (this function handles DID lookup)
      fout: string; valid .json filename
    
    Outputs:
      none; writes a JSON object (list of dicts) to fout.
    """
    resolved_handle = requests.get(
        "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
        params={"handle": handle},
    ).json()
    
    print(resolved_handle)
    did = resolved_handle["did"]
    
    has_more = True
    cursor = ""
    all_reposts = []
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
        batch = batch.json()
        all_reposts.extend(batch['records'])
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

for handle, fout in [
    ('cetaceanneeded.bsky.social', f'{FILEPATH_OUT}/bsky_reposts/cetaceanneeded.bsky.social.json'),
]:
    get_all_reposts(handle, fout)