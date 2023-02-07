"""
Basic Sync CLI
"""

import json
import sys
from argparse import ArgumentParser
from getpass import getpass

from .api import get_history, get_prediction
from .config import API_KEY, CONFIG, APIKey, Configuration, configure
from .emr import run_job_flow
from .models import Preference


def main():
    # May want to move to [click](https://click.palletsprojects.com/en/8.1.x/) at some point
    parser = ArgumentParser("Sync")
    sub_parsers = parser.add_subparsers(dest="cmd")

    config_parser = sub_parsers.add_parser("configure")
    config_parser.add_argument("--state", "-s", help="URL of state file")
    config_parser.add_argument(
        "--preference",
        "-p",
        default=Preference.BALANCED,
        type=Preference,
        choices=[p for p in Preference],
        dest="prediction_preference",
        help="default prediction preference",
    )

    autotuner_parser = sub_parsers.add_parser("autotuner")
    autotuner_parsers = autotuner_parser.add_subparsers(dest="autotuner_cmd")
    history_parser = autotuner_parsers.add_parser("history")
    history_parser.add_argument("--brief", default=False, action="store_true")

    prediction_parser = autotuner_parsers.add_parser("prediction")
    prediction_parser.add_argument("prediction_id", metavar="PREDICTION_ID")
    prediction_parser.add_argument(
        "--preference",
        "-p",
        default=Preference.BALANCED,
        type=Preference,
        choices=[p for p in Preference],
        dest="prediction_preference",
        help="default prediction preference",
    )

    emr_parser = sub_parsers.add_parser("emr")
    emr_parsers = emr_parser.add_subparsers(dest="emr_cmd")
    jobflow_parser = emr_parsers.add_parser("job")
    jobflow_parser.add_argument("conf", metavar="JOB_FLOW_CONF")

    args = parser.parse_args()

    match args.cmd:
        case "configure":
            api_key = _prompt_for_api_key()
            config = _prompt_for_configuration()
            configure(api_key, config)
        case "autotuner":
            match args.autotuner_cmd:
                case "history":
                    history = get_history()
                    if args.brief:
                        for prediction in history:
                            print(
                                f"{prediction['prediction_id']} {prediction['application_name']:>32} {prediction['created_at']}"
                            )
                    else:
                        json.dump(history, sys.stdout, indent=2)
                case "prediction":
                    json.dump(
                        get_prediction(args.prediction_id, args.prediction_preference),
                        sys.stdout,
                        indent=2,
                    )
        case "emr":
            match args.emr_cmd:
                case "job":
                    with open(args.conf) as job_flow_in:
                        job_flow = json.load(job_flow_in)
                    json.dump(run_job_flow(job_flow), sys.stdout, indent=2)


def _prompt_for_api_key():
    """
    Prompts user for API key
    """

    if API_KEY:
        api_key_id = input(f"API key ID [{API_KEY.id}]: ")
        api_key_secret = getpass("API key secret [*****]: ")

        if api_key_id and api_key_secret:
            return APIKey(api_key_id=api_key_id, api_key_secret=api_key_secret)
        return API_KEY

    api_key_id = input(f"API key ID: ")
    api_key_secret = getpass("API key secret: ")
    return APIKey(api_key_id=api_key_id, api_key_secret=api_key_secret)


def _prompt_for_configuration():
    """
    Prompts user for configuration
    """

    # TODO default to existing configuration
    project_url = input(f"Default project URL [{CONFIG.default_project_url}]: ")
    prediction_preference = getpass(
        f"Default prediction preference [{CONFIG.default_prediction_preference}]: "
    )

    return Configuration(
        default_project_url=project_url or CONFIG.default_project_url,
        default_prediction_preference=prediction_preference or CONFIG.default_prediction_preference,
    )
