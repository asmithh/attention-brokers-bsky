"""
Utility functions for data parsing
"""

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