from random import randint
import pandas as pd

from typing import Dict


def allocator(arms: pd.DataFrame) -> Dict[str, str]:
    '''Random arm decision policy'''
    try:
        random_arm = randint(a=0, b=len(arms) - 1)
        allocated_arm = arms.recommender.iloc[random_arm]
    except:
        allocated_arm = 'fallback-recommender'

    decision = {
        'allocator': 'random-arm',
        'arm': allocated_arm
    }
    return decision
