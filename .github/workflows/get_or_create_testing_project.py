"""
Script which gets or creates a DNAnexus 004 test project for the config
"""

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
    # Command line args for getting or creating a project
    parser = argparse.ArgumentParser(
        description='Info required to run testing jobs'
    )

    parser.add_argument(
        '-i',
        '--input_config',
        required=True,
        type=str,
        help="Name of the config file which has been updated"
    )

    parser.add_argument(
        '-o',
        '--output_filename',
        required=True,
        type=str,
        help="Name of the file to write the testing project ID to"
    )

    parser.add_argument(
        '-u',
        '--run_url',
        required=True,
        type=str,
        help=(
            "URL of this GitHub Actions run to be included in the testing "
            "project description"
        )
    )

    parser.add_argument(
        '-r',
        '--run_id',
        required=True,
        type=str,
        help="ID of this GitHub Actions run to name the output DX folder"
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


def time_stamp() -> str:
    """
    Returns string of date & time formatted as YYMMDD_HHMM

    Returns
    -------
    str
        String of current date and time as YYMMDD_HHMM
    """
    return datetime.now().strftime("%y%m%d_%H%M")


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args

    def find_dx_test_project(self):
        """
        Check if a 004 project already exists in DNAnexus which matches
        what the project should be called for the config file updated

        Returns
        -------
        existing_projects: list
            list containing info (in dict form) about existing project(s) if
            found
        Example:
        [
            {
                'id': 'project-Gpb3k6Q4PZYxVz9pzXvK82Xy',
                'level': 'ADMINISTER',
                'permissionSources': ['user-locker'],
                'public': False,
                'describe': {
                    'id': 'project-Gpb3k6Q4PZYxVz9pzXvK82Xy',
                    'name': (
                        '004_240731_GitHub_Actions_dias_TWE_config_GRCh37'
                        '_v3.1.7_testing'
                    ),
                    'created': 1722432666000,
                    'createdBy': {
                        'user': 'user-locker'
                    }
                }
            }
        ]

        Raises
        ------
        AssertionError
            Raised when more than one 004 project found for given config name
        """
        changed_config_name = self.args.input_config.replace('.json', '')
        # Find existing project(s) beginning with 004, with a date in %y%m%d
        # format and includes the name of the updated config file
        existing_projects = list(
            dx.find_projects(
                name=(
                    '^004_[\d]{2}[\d]{2}[\d]{2}_GitHub_Actions_'
                    f'{changed_config_name}.*$'
                ),
                name_mode='regexp',
                describe={
                    'fields': {
                        'name': True,
                        'created': True,
                        'createdBy': True
                    }
                }
            )
        )

        if existing_projects:
            existing_project_info = '\n\t'.join(
                [f"{proj['id']}: {proj['describe']['name']}"
                for proj in existing_projects]
            )

            assert len(existing_projects) == 1, (
                "Found multiple existing 004 testing projects for the updated "
                f"config file: \n\t{existing_project_info}\n. Please either"
                " rename or delete projects so a maximum of one remains"
            )

        return existing_projects

    def get_or_create_dx_project(self, existing_projects):
        """
        If a testing project for the updated config file exists, get the ID
        of that project. If one does not exist, make a new project for testing

        Parameters
        ----------
        existing_projects : list
            list containing info on any existing 004 test project for the
            config file which has been updated

        Returns
        -------
        project_id: str
            ID of the testing project to use
        """
        # Remove .json from name of updated config file
        changed_config_name = self.args.input_config.replace('.json', '')

        # If no 004 projs exist, create new proj with today's date + name of
        # the config file. This includes 'GitHub_Actions' in the name for now
        # but will be updated when the workflow is in production
        if not existing_projects:
            new_project_name = (
                f"004_{datetime.now().strftime('%y%m%d')}_GitHub_Actions_"
                f"{changed_config_name}_testing"
            )

            # Create project and capture returned project ID and store
            project_id = dx.DXProject().new(
                name=new_project_name,
                summary=(
                    f"Project for testing changes in {changed_config_name}"
                ),
                description=(
                    "This project was created automatically by GitHub Actions"
                    f": {self.args.run_url}"
                )
            )
            print(
                "\n004 project for updated config does not exist. Created new "
                f"project for testing: {new_project_name} ({project_id})"
            )

            # Add user permissions to the project
            users = {"org-emee_1": "CONTRIBUTE"}
            if users:
                # grant user(s) access to project
                for user, access_level in users.items():
                    dx.DXProject(dxid=project_id).invite(
                        user, access_level, send_email=False
                    )
                    print(
                        f"\nGranted {access_level} privilege to {user}"
                    )
        else:
            project_id = existing_projects[0]['id']
            project_info = '\n\t'.join([
                f"{x['describe']['name']} ({x['id']}) - "
                f"created by {x['describe']['createdBy']['user']} on "
                f"{datetime.fromtimestamp(x['describe']['created']/1000).strftime('%Y-%m-%d')}" for x in existing_projects
            ])

            print(
                "Project for testing the updated config file already "
                f"exists: \n{project_info}.\nUpdated config will be uploaded"
                f" to existing test project {project_id}"
            )

        return project_id

    def create_dx_folder(self, test_project_id, timestamp):
        """
        Creates a DNAnexus folder for the GitHub Actions run if it does not
        exist

        Parameters
        ----------
        test_project_id : str
            ID of the DX project to create the folder in
        timestamp : str
            the datetime in format "%y%m%d_%H%M" e.g. 220131_1234

        Returns
        -------
        folder_name : str
            The name of the folder that has been created in the DX test
            project

        """
        # Create name for the folder to put the job output in
        folder_name = f"/GitHub_Actions_run-{self.args.run_id}_{timestamp}"

        # Try and find a folder in the project which matches this name
        try:
            dx.api.project_list_folder(
                object_id=test_project_id,
                input_params={
                    'folder': folder_name,
                    'only': 'folders'
                },
                always_retry=True
            )

        except dx.exceptions.ResourceNotFound:
            # Can't find folder -> create one
            dx.api.project_new_folder(
                object_id=test_project_id,
                input_params={
                    'folder': folder_name,
                    'parents': True
                }
            )

            print(f'Created output folder: {folder_name}')

        return folder_name

    def write_project_id_to_file(self, project_id, folder_name):
        """
        Write the project ID and folder name to a file for use in the next
        steps of the workflow

        Parameters
        ----------
        project_id: str
            ID of the DX testing project which has been found/created
        folder_name: str
            name of the folder which has been created in the testing project
        """
        project_dict = {
            'project_id': project_id,
            'folder_name': folder_name

        }
        with open(self.args.output_filename, 'w', encoding='utf8') as out_file:
            json.dump(project_dict, out_file)


def main():
    """
    Run main functions for getting or creating a DX test project
    """
    args = parse_args()
    dx_manage = DXManage(args)

    existing_test_project = dx_manage.find_dx_test_project()
    project_004_id = dx_manage.get_or_create_dx_project(
        existing_projects=existing_test_project
    )
    folder_created = dx_manage.create_dx_folder(
        test_project_id=project_004_id,
        timestamp=time_stamp()
    )
    dx_manage.write_project_id_to_file(
        project_id=project_004_id,
        folder_name=folder_created
    )


if __name__ == '__main__':
    main()
