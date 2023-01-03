__author__ = 'aman.rv'

import argparse
import simplejson


def read_job_config(args):
    """
    :return:
    """
    with open(args.config_file_name) as config_file:
        try:
            return simplejson.load(config_file)
        except simplejson.errors.JSONDecodeError as error:
            raise simplejson.errors.JSONDecodeError(
                f"Issue with the Job Config: {args.config_file_name}.json {error}"
            )


def parse_known_cmd_args():
    """
    Parse Cmd known args
    Return Example: Namespace(config_file_name=job_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_file_name", help="specify config file name", action="store"
    )
    parser.add_argument("--env", help="env, dev, pre-prod, prod", action="store")
    return parser.parse_known_args()[0]