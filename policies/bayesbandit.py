from scipy import stats
import pandas as pd
import numpy as np

from typing import Dict


def bernoulli_update(trials: int, success: int, prior=(1, 1)) -> stats.beta:
    """Update bernoulli reward beta distrabution."""
    a_hat = prior[0] + success
    b_hat = prior[1] + (trials - success)
    return stats.beta.rvs(a_hat, b_hat)


def allocator(arms: pd.DataFrame) -> Dict[str, str]:
    """Bayesian bandit (thompson sampling) allocator. Represends each arms extected
    reward as a beta distrabution. Greedly allocates arms based on the largest sample
    from each arms reward distrabution."""

    scores = bernoulli_update(arms['trials'].values, arms['successes'].values, (1, 1))
    best_idx = np.argmax(scores)

    decision = {
        'allocator': 'bayesian-bandit',
        'arm': arms['recommender'].values[best_idx]
    }
    return decision
