from math import isclose
import pytest
import pandas as pd
from scipy import stats
from bt_rts.services.allocations.policies import bayesbandit
from bt_rts.services.allocations import allocation


@pytest.fixture(scope='module')
def arms():
    example_data = {
        'site_id': ['666'] * 5,
        'recommender': ['collab-basic', 'pbfm', 'item-sim', 'context-bandit', 'deep+wide-nn'],
        'trials': [10, 20, 30, 25, 100],
        'successes': [2, 6, 10, 5, 22]
    }
    return pd.DataFrame(example_data)


def test_get_arms_data(arms):
    '''tests that allocation only includes `allowed_recommenders` and
    adds any *new* recommenders to the dataset.
    '''
    updated_arms = allocation.get_arms_data(arms, allowed_recommenders=set(
        ['collab-basic', 'pbfm', 'item-sim', 'context-bandit', 'new-alpha-recer']))

    assert len(updated_arms) == 5

    assert any(updated_arms.recommender.isin(['new-alpha-recer']))

    assert not any(updated_arms.recommender.isin(['deep+wide-nn']))


def test_bernoulli_update():
    '''tests the baysian posterior update function for the bernoulli case.'''
    posterior = bayesbandit.bernoulli_update(trials=0, success=0, prior=stats.beta(1, 1))
    assert posterior.median() == 0.5

    posterior = bayesbandit.bernoulli_update(trials=100, success=80, prior=stats.beta(10, 10))
    assert isclose(a=posterior.median(), b=0.75, rel_tol=0.01)
