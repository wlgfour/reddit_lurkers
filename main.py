import datetime as dt
import random
from multiprocessing import Process, Queue
from time import sleep

import numpy as np
import pandas as pd
import requests
import json

from proxies import PROXIES

NCORES = 40
# start ----> end
start = dt.datetime(year=2020, month=1, day=1).timestamp()
end = dt.datetime(year=2019, month=1, day=1).timestamp()
delta = dt.timedelta(days=1).total_seconds()

# Import data
subs = pd.read_csv('data/2020-07-31.csv', usecols=['real_name', 'subs'])
subs = subs.nlargest(10, 'subs')
subs = subs['real_name'].to_list()
print('Top N subs: ' + ', '.join(subs))

# Define the worker function
def make_request(url, depth=0):
    i = random.randint(0, len(PROXIES) - 1)
    p = {'http': PROXIES[i]}

    # Get the data and try again if fail http error code or parse err
    exception = None
    try:
        r = requests.get(url, proxies=p, timeout=5)
        if r.status_code != 200:
            raise Exception(f'{r.status_code}')
        data = json.loads(r.text)['data']
        if len(data) == 0:
            return -1, set()
        return int(data[-1]['created_utc']), set([d['author'] for d in data])
    except Exception as e:
        exception = e

    # Wait if failure
    if depth > 10:
        print(f'Failed to get data for {url}:')
        print(exception)
        return
    else:
        sleep(np.random.rand() * 2.5)
        return make_request(url, depth=depth+1)

def scrape(dq, eq):
    """
    params:
        dq: queue to dequeue (sub, after, before) tuples from
        eq: queue to write (sub, aubmission_authors{set}, somment_authors{set}) tuples to
    """
    while True:
        if dq.empty():
            sleep(2)
            continue
        sub, after, before = dq.get()
        
        # End condition
        if sub == 'END':
            eq.put(('END', None, None))
            return


        # Submissions
        tbefore = before
        submission_authors = set()
        while tbefore != -1:
            url = f'http://api.pushshift.io/reddit/search/submission/?size=1000&' + \
                f'after={int(after)}&before={int(before)}&subreddit={sub}&' + \
                f'fields=author,created_utc&' + \
                f'sort=desc&sort_type=created_utc'
            tbefore, tsubmission_authors = make_request(url)
            submission_authors = submission_authors.union(tsubmission_authors)

        # Comments
        tbefore = before
        comment_authors = set()
        while tbefore != -1:
            url = f'http://api.pushshift.io/reddit/search/comment/?size=1000&' + \
                  f'after={int(after)}&before={int(before)}&subreddit={sub}&' + \
                  f'fields=author,created_utc&' + \
                  f'sort=desc&sort_type=created_utc'
            tbefore, tcomment_authors = make_request(url)
            comment_authors = comment_authors.union(tcomment_authors)

        # Return
        eq.put((sub, submission_authors, comment_authors))

class JobIter(object):
    """ iterate over (after, before, sub) tuples
    """
    def __init__(self, end, start, delta, subs):
        self.jobs = [(sub, start - delta, start) for sub in subs]
        self.start = start - delta
        self.end = end
        self.niters = 0

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.jobs) == 0:
            self.jobs = [(sub, start - delta, start) for sub in subs]
            print(f'\t[{self.niters}] {dt.datetime.fromtimestamp(self.start).strftime(r"%Y-%m-%d")}')
            self.start = start - delta
            self.niters += 1

        if self.start <= self.end:
            raise StopIteration()
        
        return self.jobs.pop()

# Start the workers
print('Starting jobs')
eq = Queue(int(NCORES * 1.5))
dqs = []
procs = []
for i in range(NCORES):
    q = Queue(5)
    p = Process(target=scrape, args=[q, eq])
    p.start()
    dqs.append(q)
    procs.append(p)

# Decrement start until it is end
print('Queueing time ranges')
jobs = JobIter(end, start, delta, subs)
submission_authors = {sub: set() for sub in subs}
comment_authors = {sub: set() for sub in subs}
dq_ind = 0
for job in jobs:
    # When a job is put, we break loop
    while True:
        # Enqueue -- queue might be full
        try:
            dqs[dq_ind].put(job)
            dq_ind = (dq_ind + 1) % len(dqs)
            break
        except Exception:
            pass
        
        # Dequeue
        while not eq.empty():
            sub, sub_as, com_as = eq.get()
            submission_authors[sub] = submission_authors[sub].union(sub_as)
            comment_authors[sub] = comment_authors[sub].union(com_as)

# Write 'END' to each thread, dequeue all, and wait to join
print('Joing workers...')
for q in dqs:
    q.put('END')

nends = 0
while nends < NCORES:
    sub, sub_as, com_as = eq.get()
    if sub == 'END':
        nends += 1
        print(f'\t{nends}/{NCORES}')
        continue
    submission_authors[sub] = submission_authors[sub].union(sub_as)
    comment_authors[sub] = comment_authors[sub].union(com_as)

eq.close()
for q in dqs:
    q.close()
for p in procs:
    p.join()
data = {
    'subs': [], 
    'comments_only': [], 
    'submissions_only': [], 
    'comments_and_submissions': []
    }

# Get final counts and write to file
for sub in submission_authors:
    sub_as = submission_authors[sub]
    com_as = submission_authors[sub]
    data['subs'].append(sub)
    data['comments_only'].append(len(com_as.difference(sub_as)))
    data['submissions_only'].append(len(sub_as.difference(com_as)))
    data['comments_and_submissions'].append(len(sub_as.intersection(com_as)))
    
pd.DataFrame(data)
pd.to_csv('output.csv')