"""
Script which finds the highest version of a dias batch config in DNAnexus
and writes info on this (and the config updated in the PR) to a JSON
"""

import argparse
import dxpy as dx
import json
import re

from collections import defaultdict
from packaging.version import Version


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    # Command line args set up for finding prod config
    parser = argparse.ArgumentParser(
        description='Config files to run tests for'
    )

    parser.add_argument(
        '-i',
        '--input_config',
        required=True,
        type=str,
        help="Config JSON file which was changed in PR"
    )

    parser.add_argument(
        '-f',
        '--file_id',
        required=True,
        type=str,
        help='File ID of changed config file in DNAnexus'
    )

    parser.add_argument(
        '-o',
        '--output_file',
        required=False,
        default='config_diff.json',
        type=str,
        help='Name of JSON file with info on updated vs prod config'
    )

    parser.add_argument(
        '-p',
        '--config_path',
        required=True,
        type=str,
        help=(
            'Path to production config files in DNAnexus - in format '
            '<project_id>:/path'
        )
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

    def read_in_config(self):
        """
        Read in JSON file to a dict

        Returns
        -------
        config_dict: dict
            the content of the JSON converted to a dict
        assay: str
            name of the assay the updated config relates to, e.g. 'TWE'

        Raises
        ------
        AssertionError
            Raised when there is no assay field in the updated config
        """
        if not self.args.input_config.endswith('.json'):
            raise RuntimeError(
                'Error: invalid config file given - not a JSON file'
            )

        with open(self.args.input_config, 'r', encoding='utf8') as json_file:
            config_contents = json.load(json_file)

        # Add in keys for the file name and DX file ID to make diffing easier
        # later
        config_contents['name'] = self.args.input_config
        config_contents['dxid'] = self.args.file_id

        # Get the assay field from the config
        assay = config_contents.get('assay')
        assert assay, (
            f"The updated config {self.args.input_config} does not have "
            "assay field"
        )

        return config_contents, assay

    @staticmethod
    def get_json_configs_in_DNAnexus(config_path) -> dict:
        """
        Query path in DNAnexus for JSON config files, returning all unarchived
        config files found

        Parameters
        ----------
        config_path : str
            path to prod config files in DNAnexus (<project_id>:/path format)

        Returns
        -------
        config_files: list
            list of dicts, each with info about a JSON config file found in
            the DX path

        Raises
        ------
        AssertionError
            Raised when invalid project:path structure given
        AssertionError
            Raised when no config files found at the given path
        """
        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', config_path), (
            f'Path to assay configs appears invalid: {config_path}'
        )

        print(f"\n \nSearching following path for assay configs: {config_path}")

        project, path = config_path.split(':')

        config_files = list(dx.find_data_objects(
            name=".json$",
            name_mode='regexp',
            project=project,
            folder=path,
            describe=True
        ))

        # Sense check we find config files
        assert config_files, print(
            f"No config files found in given path: {project}:{path}"
        )

        files_ids = '\n\t'.join([
            f"{x['describe']['name']} ({x['id']} - "
            f"{x['describe']['archivalState']})" for x in config_files])
        print(f"\nAssay config files found:\n\t{files_ids}")

        return config_files

    def filter_highest_batch_config(self, config_files, assay):
        """
        Get the highest version of the prod dias batch config for the
        relevant assay

        Parameters
        ----------
        config_files : list
            list of dicts, each with info about a JSON config file found in
            the DX path
        assay : str
            the assay we're looking for the highest version of (e.g. 'TWE)

        Returns
        -------
        highest_config : dict
            the contents of the prod config with the highest version for that
            assay

        Raises
        ------
        AssertionError
            Raised when no config file is found for the assay in the path
        RunTimeError
            Raised when more than one file found for highest version of
            the config for the assay
        """
        highest_config = {}
        config_version_files = defaultdict(list)

        # Loop over files, ignoring archived configs and those for
        # different assay
        for file in config_files:
            if not file['describe']['archivalState'] == 'live':
                print(
                    "Config file not in live state - will not be used:"
                    f"{file['describe']['name']} ({file['id']}"
                )
                continue

            # Read in to dict
            config_data = json.loads(
                dx.DXFile(
                    project=file['project'],
                    dxid=file['id']
                ).read())

            if not config_data.get('assay') == assay:
                continue

            # build a log of files found for each version to ensure we
            # only find one of each version to unambiguously get highest
            config_version_files[config_data.get('version')].append(
                (file['describe']['name'], file['id'])
            )

            if Version(config_data.get('version')) > Version(
                highest_config.get('version', '0')
            ):
                config_data['dxid'] = file['id']
                config_data['name'] = file['describe']['name']
                highest_config = config_data

        assert highest_config, (
            f"No config file was found for {assay} in {self.args.config_path}"
        )

        if len(config_version_files[highest_config['version']]) > 1:
            files = '\n\t'.join([
                f"{x[0]} ({x[1]})"
                for x in config_version_files[highest_config['version']]
            ])

            raise RuntimeError(
                f"Error: more than one file found for highest version of "
                f"{assay} configs. Files found:\n\t{files}"
            )

        print(
            f"Highest version config found for {assay} is "
            f"{highest_config.get('version')} -> {highest_config.get('dxid')}"
        )

        return highest_config

    @staticmethod
    def save_config_info_to_dict(assay, changed_config, prod_config):
        """
        Save info about the updated and prod configs to dict

        Parameters
        ----------
        changed_config : dict
            contents of the changed config JSON file as a dict
        prod_config : dict
            contents of the highest prod config JSON as a dict

        Returns
        -------
        config_matching_info : dict
            dict with info on the config which has been updated and the
            highest prod config
        """
        config_matching_info = {
            'assay': assay,
            'updated': {
                'name': changed_config.get('name'),
                'assay': changed_config.get('assay'),
                'version': changed_config.get('version'),
                'dxid': changed_config.get('dxid')
            },
            'prod': {
                'name': prod_config.get('name'),
                'assay': prod_config.get('assay'),
                'version': prod_config.get('version'),
                'dxid': prod_config.get('dxid')
            }
        }

        print("\n \nBatch configs that will be compared:")
        prettier_print(config_matching_info)

        return config_matching_info

    def write_out_config_info(self, changed_config_to_prod):
        """
        Write out the updated vs prod config dictionary to a JSON file

        Parameters
        ----------
        changed_config_to_prod : dict
            dict with info on the config which has been updated and the
            highest prod config
        file_name: str
            name of JSON file to write out
        """
        with open(self.args.output_file, 'w', encoding='utf8') as json_file:
            json.dump(changed_config_to_prod, json_file, indent=4)


def main():
    """
    Run functions to create JSON with info on updated vs prod config
    """
    args = parse_args()
    dx_manage = DXManage(args)

    changed_config_contents, assay = dx_manage.read_in_config()
    config_files = dx_manage.get_json_configs_in_DNAnexus(args.config_path)
    prod_config = dx_manage.filter_highest_batch_config(
        config_files,
        assay
    )
    changed_and_prod_config_info = dx_manage.save_config_info_to_dict(
        assay,
        changed_config_contents,
        prod_config
    )
    dx_manage.write_out_config_info(changed_and_prod_config_info)

if __name__ == '__main__':
    main()
