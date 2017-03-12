from thriftpy.rpc import make_client
from bt_rts.thrift.gen.allocations_thrift import TAllocation, Allocations

from typing import Set


class AllocationsClient(object):
    def __init__(self, host='localhost', port=7070):
        self._client = make_client(Allocations, host, port)

    def select_recommender(self, site_id: str, bsin: str, allowed_recommenders: Set[str]) -> TAllocation:
        if not allowed_recommenders:
            raise ValueError('need a value!')
        elif isinstance(allowed_recommenders, (list, tuple)):
            allowed_recommenders = set(allowed_recommenders)
        return self._client.select_recommender(site_id, bsin, allowed_recommenders)


if __name__ == '__main__':
    SITE = '666'
    BSIN = 'foo'
    OPTIONS = set(['collab-basic', 'item-sim', 'pbfm', 'pbfm-v3'])

    client = AllocationsClient()
    print(client.select_recommender(SITE, BSIN, OPTIONS))
