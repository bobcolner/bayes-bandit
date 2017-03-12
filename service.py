"""service.py 

Usage: 
    service.py -C <config-path>
    service.py -h | --help

Options:
    -h --help                          Show this screen
    -C --config-path <config-path>     path to config file
"""
import cachetools
import pandas as pd
from docopt import docopt
from redis import StrictRedis
from thriftpy.rpc import make_server
from bt_rts.services.allocations import allocation
from bt_rts.services.allocations import feedback
from bt_rts.services.allocations.experiments import DefaultExp
from bt_rts.thrift.gen.allocations_thrift import TAllocation, Allocations

from typing import Set, Dict


class AllocationService(object):
    def __init__(self):
        pass

    @classmethod
    def serve(cls, config):
        dispatcher = cls()
        server = make_server(Allocations, dispatcher, **config['thrift'])
        server.serve()

    def select_recommender(self, site_id: str,
                           bsin: str,
                           allowed_recommenders: Set[str]) -> TAllocation:
        """Returns an recommender allocation decision based on historic feedback."""
        if type(allowed_recommenders) != set:
            allowed_recommenders = set(allowed_recommenders)
            print('needed to cast `allowed_recommenders` to set..')

        decision = _select_recommender(site_id, bsin, allowed_recommenders)
        print(decision)
        return TAllocation(allocator=decision['allocator'], recommender=decision['arm'])


def _select_recommender(site_id: str,
                        bsin: str,
                        allowed_recommenders: Set[str]) -> Dict[str, str]:
    # get users experiment params
    exp_params = DefaultExp(bsin=bsin).get_params()
    # get existing arms data (trials & successes)
    arms = get_existing_arms_data(site_id)
    return allocation.select_arm(arms, exp_params['allocator'], bsin, allowed_recommenders)


@cachetools.cached(cache=cachetools.TTLCache(maxsize=999, ttl=300))
def get_existing_arms_data(site_id: str) -> pd.DataFrame:
    """Query Allocator DB, return all recommender trials per site_id
        TODO: implement real DB query
    """
    return feedback.enumerate_keys(site_ids=list(site_id), experiments=['default'], recommender_ids=['fmv1', 'collb'])


if __name__ == '__main__':
    print('running allocations service')
    # CLI arg parsing
    arguments = docopt(__doc__)

    # config = {
    #     'thrift': {
    #         'host': 'localhost',
    #         'port': 7070
    #     }
    # }
    AllocationService.serve(config)
