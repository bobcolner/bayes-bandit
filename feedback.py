from datetime import datetime, timedelta
import json
import pandas as pd
from redis import StrictRedis
import mmh3
from docopt import docopt
from typing import List, Dict

INT_MAX = (1 << 32) - 1
HOUR_FORMAT = '%Y%m%d%H'


class Feedback(object):

    def __init__(self, config=None):
        if config is None:
            config = {}
        self.redis = StrictRedis(**config['redis'])

    def get_allocation_key(self, site_id: str, experiment: str, recommender_id: str, allocation_time: datetime=None) -> str:
        if allocation_time is None:
            allocation_time = datetime.utcnow()

        long_key = '/'.join([site_id, experiment, recommender_id])
        short_key = '{:08x}'.format(mmh3.hash(long_key) & INT_MAX)

        timestamp = allocation_time.strftime(HOUR_FORMAT)
        return short_key + ':' + timestamp

    def insert(self, allocation_key: str, event_type: str, rec_id: str, ttl=timedelta(days=60)):
        full_key = event_type + ':' + allocation_key
        key_exists = self.redis.exists(full_key)

        self.redis.execute_command('PFADD', full_key, rec_id)

        if not key_exists:
            timestamp = allocation_key.rsplit(':', 1)[-1]
            allocation_time = datetime.strptime(timestamp, HOUR_FORMAT)
            expire_time = allocation_time + ttl
            self.redis.expireat(full_key, expire_time)

    def count_distinct(self, allocation_key: str, event_type: str) -> int:
        return self.redis.execute_command('PFCOUNT', event_type + ':' + allocation_key)

    def increment_arm(self, event: Dict):
        key = self.get_allocation_key(site_id=event['site_id'],
                                      experiment=event['experiment'],
                                      recommender_id=event['recommender_id'],
                                      allocation_time=event['allocation_time']
                                      )
        rec_id = event.get('recset', 'NA') + ':' + event.get('rec_group', 'NA') + ':' + event.get('rec_position', 'NA')
        self.insert(key, event['event_type'], rec_id)

    def flush_redis(self):
        self.redis.flushall()

    def event_listener(self):
        '''TODO: integrate with kafka --currently stubbed'''

        event_stream = iter([
            dict(site_id='666', event_type='rec_viewed', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='666', event_type='rec_viewed', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='666', event_type='rec_viewed', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='666', event_type='rec_clicked', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='666', event_type='rec_clicked', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='666', event_type='rec_clicked', rec_time=datetime(2016, 11, 1, 12),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='fmv1', experiment='default'),
            dict(site_id='238', event_type='rec_viewed', rec_time=datetime(2016, 11, 2, 8),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='collb', experiment='default'),
            dict(site_id='238', event_type='rec_clicked', rec_time=datetime(2016, 11, 2, 8),
                 recset='abc', rec_group='a', rec_position='1', recommender_id='collb', experiment='default')
        ])

        for event in event_stream:
            if event['event_type'] in ['rec_viewed', 'rec_clicked']:
                self.increment_arm(event)

    def perdelta(self, start: datetime, end: datetime, delta: timedelta) -> List[datetime]:
        output = []
        curr = start
        while curr < end:
            curr = curr + delta
            output.append(curr)
        return output

    def get_recent_hours(self, days_ago: int=30) -> List[datetime]:
        end_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_hour = end_hour - timedelta(days=days_ago)
        return self.perdelta(start_hour, end_hour, timedelta(hours=1))

    def enumerate_keys(self, site_ids: List[str], experiments: List[str], recommender_ids: List[str]) -> pd.DataFrame:
        header = ['site_id', 'experiment', 'recommender_id', 'date_hour', 'trials', 'successes']
        rows = []
        for site_id in site_ids:
            for experiment in experiments:
                for recommender_id in recommender_ids:
                    for date_hour in self.get_recent_hours():
                        key = self.get_allocation_key(site_id, experiment, recommender_id, date_hour)
                        view_count = self.count_distinct(key, 'rec_viewed')
                        click_count = self.count_distinct(key, 'rec_clicked')
                        rows.append([site_id, experiment, recommender_id, date_hour, view_count, click_count])
        df = pd.DataFrame(data=rows, columns=header)
        # TODO: date filter
        return df.groupby(['site_id', 'experiment', 'recommender_id']).sum().reset_index()


if __name__ == '__main__':

    args = docopt(__doc__)

    if args['--config_path'] is None:
        config_path = 'dev_config.json'
    else:
        config_path = args['--config_path']

    with open(config_path) as fio:
        config = json.load(fio)

    feedback_worker = Feedback(config)

    feedback_worker.event_listener()
