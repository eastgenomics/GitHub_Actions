import argparse
import dxpy as dx
import json
import re

from collections import defaultdict
from packaging.version import Version, parse


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
    def read_in_json(config_file, file_id) -> dict:
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
        if not config_file.endswith('.json'):
            raise RuntimeError(
                'Error: invalid config file given - not a JSON file'
            )

        with open(config_file, 'r', encoding='utf8') as json_file:
            config_contents = json.load(json_file)

        config_contents['name'] = config_file
        config_contents['dxid'] = file_id

        return config_contents

    def get_json_configs_in_DNAnexus(self, config_path) -> dict:
        """
        Query path in DNAnexus for json config files for each assay, returning
        full data for all unarchived config files found

        ASSAY_CONFIG_PATH comes from the app config file sourced to the env.

        Returns
        -------
        all_configs: list
            list of dicts of the json object for each config file found

        Raises
        ------
        AssertionError
            Raised when invalid project:path structure defined in app config
        AssertionError
            Raised when no config files found at the given path
        """
        # searching dir for configs, check for valid project:path structure
        assert re.match(r'project-[\d\w]*:/.*', config_path), (
            f'Path to assay configs appears invalid: {config_path}'
        )

        print(f"\n \nSearching following path for assay configs: {config_path}")

        project, path = config_path.split(':')

        files = list(dx.find_data_objects(
            name=".json$",
            name_mode='regexp',
            project=project,
            folder=path,
            describe=True
        ))

        # sense check we find config files
        assert files, print(
            f"No config files found in given path: {project}:{path}"
        )

        files_ids = '\n\t'.join([
            f"{x['describe']['name']} ({x['id']} - "
            f"{x['describe']['archivalState']})" for x in files])
        print(f"\nAssay config files found:\n\t{files_ids}")

        return files

    @staticmethod
    def filter_highest_batch_config(config_path, config_files, assay):
        """
        _summary_

        Returns
        -------
        _type_
            _description_
        """
        highest_config = {}
        config_version_files = defaultdict(list)

        for file in config_files:
            if not file['describe']['archivalState'] == 'live':
                print(
                    "Config file not in live state - will not be used:"
                    f"{file['describe']['name']} ({file['id']}"
                )
                continue

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

            if Version(config_data.get('version')) > Version(highest_config.get('version', '0')):
                config_data['dxid'] = file['id']
                config_data['name'] = file['describe']['name']
                highest_config = config_data

        assert highest_config, (
            f"No config file was found for {assay} from {config_path}"
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
            f"Highest version config found for {assay} was "
            f"{highest_config.get('version')} from {highest_config.get('dxid')}"
        )

        print("Assay config file contents:")
        prettier_print(highest_config)

        return highest_config

    @staticmethod
    def save_config_info(changed_config, prod_config):
        """
        _summary_

        Parameters
        ----------
        changed_config : _type_
            _description_
        prod_config : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        config_matching_info = {
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

    def write_out_config_info(self, changed_config_to_prod, file_name):
        """
        Write out the updated vs prod config dictionary to a JSON file

        Parameters
        ----------
        changed_config_to_prod : dict
            _description_
        file_name: str
            name of JSON file to write out
        """
        with open(file_name, 'w', encoding='utf8') as json_file:
            json.dump(changed_config_to_prod, json_file, indent=4)


def main():
    args = parse_args()
    dx_manage = DXManage(args)

    config_path = (
        "project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/"
        "dias_batch_configs"
    )
    changed_config_contents = dx_manage.read_in_json(
        args.input_config,
        args.file_id
    )
    config_files = dx_manage.get_json_configs_in_DNAnexus(config_path)
    prod_config = dx_manage.filter_highest_batch_config(
        config_path,
        config_files,
        changed_config_contents.get('assay')
    )
    changed_and_prod_config_info = dx_manage.save_config_info(
        changed_config_contents,
        prod_config
    )
    dx_manage.write_out_config_info(
        changed_and_prod_config_info,
        args.output_file
    )

if __name__ == '__main__':
    main()
