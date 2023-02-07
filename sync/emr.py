"""
Utilities for interacting with EMR
"""

import logging
from urllib.parse import urlparse

import boto3 as boto

logger = logging.getLogger(__name__)


def run_job_flow(job_flow: dict) -> str:
    """Creates an EMR job flow

    Args:
        job_flow (dict): job flow spec

    Returns:
        str: job flow ID
    """
    # Create event log dir if configured - Spark requires a directory
    for config in job_flow["Configurations"]:
        if config["Classification"] == "spark-defaults":
            if (eventlog_dir := config["Properties"].get("spark.eventLog.dir")) and config[
                "Properties"
            ].get("spark.eventLog.enabled", "false").lower() == "true":
                parsed_url = urlparse(eventlog_dir)
                if parsed_url.scheme == "s3a":
                    s3 = boto.client("s3")
                    s3.put_object(Bucket=parsed_url.netloc, Key=parsed_url.path.lstrip("/"))
                    logger.info(f"Created event log dir at {eventlog_dir}")
                    break

    emr = boto.client("emr")
    return emr.run_job_flow(**job_flow)["JobFlowId"]
