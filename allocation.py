import pandas as pd
from bt_rts.services.allocations.policies import bayesbandit
from bt_rts.services.allocations.policies import random

from typing import Set, Dict
from types import FunctionType


def select_arm(site_id: str,
               allocator_name: str,
               bsin: str,
               allowed_recommenders: Set[str]) -> Dict[str, str]:
    # get allocator based on experiment params
    allocator = get_allocator(allocator_name)
    # get feedback data for all arms
    arms = get_arms_data(site_id, allowed_recommenders)
    # decide which arm to allocate
    return allocator(arms)


def get_allocator(allocator_name: str) -> FunctionType:
    """Returns an allocator function."""
    if allocator_name == 'bayesian-bandit':
        return bayesbandit.allocator
    elif allocator_name == 'random':
        return random.allocator
    else:
        # defualt allocator
        return bayesbandit.allocator


def get_arms_data(arms: pd.DataFrame, allowed_recommenders: Set[str]) -> pd.DataFrame:
    """Get feedback data for existing arms filtered to only include `allowed_recommenders`.
    Add any new `allowed_recommenders` to the dataset (w/o feedback).
    """
    # filter existing arms not currently allowed
    allowed_arms = arms[arms.recommender.isin(allowed_recommenders)]
    existing_recommenders = set(allowed_arms.recommender)
    # add any new recommenders to feedback dataset
    new_recommenders = allowed_recommenders.difference(existing_recommenders)
    # append new recommenders to existing 'arms'
    new_arms = pd.DataFrame({
        'site_id': [arms.site_id.iloc[0]] * len(new_recommenders),
        'recommender': list(new_recommenders),
        'trials': [0] * len(new_recommenders),
        'successes': [0] * len(new_recommenders)
    })
    return allowed_arms.append(new_arms, ignore_index=True)
