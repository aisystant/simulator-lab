"""Simulator scenarios registry — WP-319.

Новый сценарий = новый файл SN.py + запись в ALL_SCENARIOS. Больше ничего менять не нужно.
"""
from activity_hub.engines.simulator.scenarios.s1_stage_trajectory import S1StageTrajectory
from activity_hub.engines.simulator.scenarios.s2_rewards_trajectory import S2RewardsTrajectory
from activity_hub.engines.simulator.scenarios.s3_cohort_dynamics import S3CohortDynamics

ALL_SCENARIOS = {
    "s1": S1StageTrajectory(),
    "s2": S2RewardsTrajectory(),
    "s3": S3CohortDynamics(),
}

__all__ = ["ALL_SCENARIOS", "S1StageTrajectory", "S2RewardsTrajectory", "S3CohortDynamics"]
