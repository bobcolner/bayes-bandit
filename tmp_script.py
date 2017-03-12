# stubbed event stream
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


redis.flushall()

for event in event_stream:
    if event['event_type'] in ['rec_viewed', 'rec_clicked']:
        increment_arm(event)

df = enumerate_keys(site_ids=['666', '238'], experiments=['default'], recommender_ids=['fmv1', 'collb'])

hll_count_distinct(get_allocation_key('666', 'default', 'fmv1', datetime(2016, 10, 1, 12)), 'rec_viewed')
hll_count_distinct(get_allocation_key('666', 'default', 'fmv1', datetime(2016, 10, 1, 12)), 'rec_clicked')

site_id = '666'
experiment = 'default'
recommender_id = 'fmv1'
allocation_time = datetime(2016, 11, 1, 12)
key = get_allocation_key(site_id, experiment, recommender_id, allocation_time)


%load_ext autoreload
%autoreload 2
import resource2vec as r2v

mongo_user = 'api-resources'
mongo_pass = 'vjw4akBHVsGoMUiAsLrJtox6Xzf'
# site_id = '1a1e951c0be6ac5f7a57c617f1160972'  # Rappler
site_id ='2a9897b9f56088c2916bb3403cfff631'  # Kellogs
collection = r2v.get_resources_collection(site_id, mongo_user, mongo_pass)

resources = r2v.get_resources(site_id, mongo_user, mongo_pass)
resource_vecs = r2v.resource2vec(resources, 'body')

pca_estimates = r2v.estimate_pca_components(resource_vecs, max_docs=999)  # find pca comps via cross-validation

pca_model = r2v.get_pca_model(resource_vecs, n_components=20)

pca_vecs = r2v.apply_pca(resource_vecs, pca_model)

# PYTHONPATH='.' luigi --module tasks LatestHourControlBuild --local-scheduler

# PYTHONPATH='.' luigi --module resourece2vec_tasks TrainPCATask --local-scheduler --target s3
# PYTHONPATH='.' luigi --module resourece2vec_tasks Resource2VecTask --local-scheduler
# PYTHONPATH='.' luigi --module resourece2vec_tasks EstimatePCACompsTask --local-scheduler
# ipython /usr/local/bin/luigi -i -- --module resourece2vec_tasks TrainPCATask --local-scheduler --site-id xxx --doc-type title+body
# ipython /usr/local/bin/luigi -i -- --module resourece2vec_tasks Resource2VecTask --local-scheduler

# ipython /usr/local/bin/luigi -i -- --module resourece2vec_tasks Resource2VecTask --local-scheduler --site-id 1a1e951c0be6ac5f7a57c617f1160972 --doc-type title+body --n-components 10

# import ipdb
# ipdb.set_trace()

from resourece2vec_tasks import Resource2VecTask
import msgpack

job = Resource2VecTask()
fp = job.output().open('r')
out = msgpack.Unpacker(fp, encoding='UTF8')
out.__next__()


def stubbed_data():
    import pandas as pd
    example_data = {
        'site_id': [site_id] * 5,
        'recommender': ['collab-basic', 'pbfm', 'item-sim', 'context-bandit', 'deep+wide-nn'],
        'experiment': ['default', 'default', 'default', 'default', 'default'],
        'trials': [10, 20, 30, 25, 100],
        'successes': [2, 6, 10, 5, 22]
    }
    return pd.DataFrame(example_data)
