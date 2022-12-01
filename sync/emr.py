"""
Utilities for interacting with EMR
"""

import boto3 as boto


def run_job_flow(job_flow: dict) -> str:
    """Creates an EMR job flow

    Args:
        job_flow (dict): job flow spec

    Returns:
        str: job flow ID
    """

    emr = boto.client("emr")
    return emr.run_job_flow(**job_flow)["JobFlowId"]
