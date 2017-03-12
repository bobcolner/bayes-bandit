from planout.experiment import SimpleExperiment
from planout.ops import random


class DefaultExp(SimpleExperiment):
    """Default placeholder allocator experiment."""

    def assign(self, params, bsin):

        params.allocator = random.UniformChoice(
            choices=['bayesian-bandit'],
            unit=bsin
        )


class MultiFactorExp(SimpleExperiment):
    """Example multi-factor experiment design."""

    def assign(self, params, bsin):

        params.allocator = random.UniformChoice(
            choices=['bayesian-bandit', 'ϵ-greedy|0.1', 'ϵ-greedy|0.2'],
            unit=bsin
        )

        params.new_infrastructure = random.WeightedChoice(
            choices=['production', 'canary'],
            weights=[0.9, 0.1],
            unit=bsin
        )
