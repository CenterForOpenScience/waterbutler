#!/usr/bin/env python
import itertools
import requests
import time
import os

"""
   Run all combinations of 's3', 'box', 'dropbox', 'github', 'googledrive',
'osfstorage' through RunScope test '006 - Charizard: cross_provider_moves.'
Environment variable 'RUNSCOPE_API_TOKEN' must be set to a valid RunScope
API token for this script to work.
"""

providers = ['s3', 'box', 'dropbox', 'github', 'googledrive', 'osfstorage']
buckets_url = 'https://api.runscope.com/buckets'
headers = {'Authorization': 'Bearer ' + os.environ['RUNSCOPE_API_TOKEN']}
bucket_name = 'WaterButler Staging'
test_name = '006 - charizard: cross provider move/copy'

resp = requests.get(buckets_url, headers=headers)
data = resp.json()
for bucket in data['data']:
    if bucket['name'] == bucket_name:
        tests_url = bucket['tests_url']
        break

resp = requests.get(tests_url, headers=headers)
data = resp.json()
for test in data['data']:
    if test['name'] == test_name:
        trigger_url = test['trigger_url']
        environment_id = test['default_environment_id']
        break

for combo in itertools.chain(itertools.combinations(providers, 2),
                             itertools.combinations(reversed(providers), 2)):
    resp = requests.get(trigger_url +
                        '?runscope_environment=' + environment_id +
                        '&source_provider=' + combo[0] +
                        '&dest_provider=' + combo[1])
    print('#######################')
    data = resp.json()
    status = data['meta']['status']
    test_name = data['data']['runs'][0]['test_name']
    test_id = data['data']['runs'][0]['test_id']
    test_run_id = data['data']['runs'][0]['test_run_id']
    bucket_key = data['data']['runs'][0]['bucket_key']
    results_url = data['data']['runs'][0]['url']
    result_url = 'https://api.runscope.com/buckets/' + bucket_key + '/tests/' + test_id + '/results/' + test_run_id

    print('Test {}'.format(test_name))
    print('Test run for {} to {}'.format(combo[0], combo[1]))
    if not status == 'success':
        print('Test start failed')
        print("Response is: {}".format(resp.text))
        continue
    print('Test run started successfuly.')
    while True:
        print('Test running check back in 60 seconds')
        time.sleep(60)
        resp = requests.get(result_url, headers=headers)
        data = resp.json()
        if data['data']['result'] == 'working':
            continue
        else:
            result = data['data']['result']
            break
    if result == 'pass':
        print('Test passed for {} to {}'.format(combo[0], combo[1]))
        continue
    elif result == 'fail':
        print('Test failed for {} to {}'.format(combo[0], combo[1]))
        print('Check results at {}'.format(results_url))
        continue
    else:
        print('Unexpected response: {} {}'.format(result, data['error']))
