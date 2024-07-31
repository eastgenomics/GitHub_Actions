import argparse
import dxpy as dx
import json

from datetime import datetime


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    # Command line args set up for running tests
    parser = argparse.ArgumentParser(
        description='Info required to run testing jobs'
    )

    parser.add_argument(
        '-c',
        '--config_info',
        required=True,
        type=str,
        help="JSON file containing info about changed and prod configs"
    )

    parser.add_argument(
        '-p',
        '--project_id',
        required=False,
        type=str,
        help="ID of the project containing "
    )

    return parser.parse_args()


def prettier_print(thing) -> None:
    """
    Pretty print for nicer viewing in the logs since pprint does not
    do an amazing job visualising big dicts and long strings

    Bonus: we're indenting using the Braille Pattern Blank U+2800
    unicode character since the new DNAnexus UI (as of Dec. 2023)
    strips leading tabs and spaces in the logs, which makes viewing
    the pretty dicts terrible. Luckily they don't strip other
    whitespace characters, so we can get around them yet again making
    their UI worse.

    Parameters
    ----------
    thing : anything json dumpable
        thing to print
    """
    print(json.dumps(thing, indent='⠀⠀'))


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args

    @staticmethod
    def read_in_json(config_info_file) -> dict:
        """
        Read in config JSON file to a dict

        Parameters
        ----------
        config_file : str
            name of JSON config file to read in

        Returns
        -------
        config_dict: dict
            the content of the JSON converted to a dict
        """
        if not config_info_file.endswith('.json'):
            raise RuntimeError(
                'Error: invalid config file given - not a JSON file'
            )

        with open(config_info_file, 'r', encoding='utf8') as json_file:
            json_contents = json.load(json_file)

        return json_contents

    @staticmethod
    def find_dias_batch_jobs(project_id):
        """
        Find completed dias batch jobs in a project

        Parameters
        ----------
        project_id : str
            ID of the project to search for batch jobs in

        Returns
        -------
        batch_jobs: list
            list of dicts containing info about batch jobs in the project
        """
        batch_jobs = list(
            dx.find_jobs(
                project=project_id,
                name='*eggd_dias_batch*',
                name_mode='glob',
                state='done',
                describe=True
            )
        )

        return batch_jobs

def main():
    """
    Run main functions for setting off testing jobs
    """
    args = parse_args()
    dx_manage = DXManage(args)
    config_info = dx_manage.read_in_json(args.config_info)
    dx_manage.find_dias_batch_jobs(args.project_id)

if __name__ == '__main__':
    main()
