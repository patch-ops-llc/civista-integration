#!/usr/bin/env python3
"""
Wipe ALL records in the 6 HubSpot CRM objects this app manages.

DANGER: this archives every contact, company, deposit, loan, time-deposit,
and debit-card in the portal pointed at by HUBSPOT_API_KEY. Use only on a
sandbox/child portal, and only when you intend to reset to zero.

Run via Railway shell so the key never leaves Railway env vars:

    railway run --service=civista-integration python scripts/wipe_sandbox.py

Idempotent — keeps paginating + archiving until each object reports 0.
The HubSpot search index can lag a few seconds behind archives, so the
final tally may show small non-zero counts that resolve on subsequent
polls.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

KEY = os.environ.get('HUBSPOT_API_KEY')
if not KEY:
    print('HUBSPOT_API_KEY env var is required', file=sys.stderr)
    sys.exit(1)

BASE = 'https://api.hubapi.com'

OBJECTS = [
    ('contacts',    'Contacts'),
    ('companies',   'Companies'),
    ('2-60442978',  'Deposits'),
    ('2-60442977',  'Loans'),
    ('2-60442980',  'Time Deposits'),
    ('2-60442979',  'Debit Cards'),
]


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header('Authorization', f'Bearer {KEY}')
    req.add_header('Content-Type', 'application/json')
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(8):
        try:
            with urllib.request.urlopen(req, data=data, timeout=30) as r:
                txt = r.read().decode()
                return r.status, json.loads(txt) if txt else {}
        except urllib.error.HTTPError as e:
            txt = e.read().decode()
            if e.code == 429:
                wait = 0.5 * (2 ** attempt)
                print(f'  429, backoff {wait}s', flush=True)
                time.sleep(wait)
                continue
            try:
                body = json.loads(txt)
            except Exception:
                body = {'raw': txt}
            return e.code, body
        except urllib.error.URLError as e:
            wait = 0.5 * (2 ** attempt)
            print(f'  network err {e}, retry {wait}s', flush=True)
            time.sleep(wait)
    return 0, {'error': 'gave up after retries'}


def list_ids(obj, limit=100):
    """Page through every record id via the /list endpoint."""
    after = None
    while True:
        path = f'/crm/v3/objects/{obj}?limit={limit}'
        if after:
            path += f'&after={after}'
        status, body = call('GET', path)
        if status >= 400:
            print(f'  list error {status}: {body.get("message", body)}', flush=True)
            return
        for r in body.get('results', []):
            yield r['id']
        paging = body.get('paging') or {}
        nxt = paging.get('next') or {}
        after = nxt.get('after')
        if not after:
            return


def batch_archive(obj, ids):
    body = {'inputs': [{'id': i} for i in ids]}
    return call('POST', f'/crm/v3/objects/{obj}/batch/archive', body)


def total(obj):
    s, b = call('POST', f'/crm/v3/objects/{obj}/search', {'limit': 1})
    return b.get('total', '?')


def wipe(obj, label):
    print(f'\n=== {label} ({obj}) ===', flush=True)
    pre = total(obj)
    print(f'  before: {pre}', flush=True)
    if pre == 0:
        return
    while True:
        ids = []
        for i in list_ids(obj, limit=100):
            ids.append(i)
            if len(ids) >= 100:
                break
        if not ids:
            break
        s, b = batch_archive(obj, ids)
        if s >= 400:
            print(f'  archive err {s}: {b.get("message", str(b)[:200])}', flush=True)
            time.sleep(2)
            continue
        time.sleep(0.1)  # gentle on the rate limiter
    post = total(obj)
    print(f'  after:  {post}', flush=True)


def main():
    for obj, label in OBJECTS:
        wipe(obj, label)
    print('\n=== final tally ===', flush=True)
    for obj, label in OBJECTS:
        print(f'  {label:15s}: {total(obj)}', flush=True)


if __name__ == '__main__':
    main()
