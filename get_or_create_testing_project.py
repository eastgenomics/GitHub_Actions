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
    def find_dx_project(changed_config_name):
        """
        Check if a 004 project already exists in DNAnexus which matches
        what the project should be called for the config file updated

        Parameters
        ----------
        changed_config_name : str
            name of the config file which has been updated
            e.g. dias_TWE_config_GRCh37_v3.1.7

        Returns
        -------
        existing_projects: list
            list containing info (in dict form) about existing project(s) if
            found, else returns None

        Raises
        ------
        AssertionError
            Raised when more than one project found for given name
        """
        # Find existing project(s) beginning with 004, with a date in %y%m%d
        # format and includes the name of the updated config file
        existing_projects = list(
            dx.find_projects(
                name=(
                    '^004_[\d]{2}[\d]{2}[\d]{2}_'
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

        if not existing_projects:
            return None

        assert len(existing_projects) == 1, (
            "Found multiple existing 004 testing projects for the updated "
            "config file"
        )

        return existing_projects

    def get_or_create_dx_project(self, changed_config_name, actions_url):
        """
        If a testing project for the updated config file exists, get the ID
        of that project. If one does not exist, make a new project for testing

        Parameters
        ----------
        changed_config_name : str
            name of the config file which has been updated
            e.g. dias_TWE_config_GRCh37_v3.1.7
        actions_url : str
            URL of this GitHub Actions run to be included in the testing
            project description

        Returns
        -------
        project_id: str
            ID of the testing project to use
        """
        # Try and find any existing projects for the updated config
        existing_projects = self.find_dx_project(changed_config_name)

        # If none exist, create new project with today's date and name of
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
                    f": {actions_url}"
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
                    prettier_print(
                        f"\nGranted {access_level} privilege to {user}"
                    )
        else:
            print(
                "Project for testing the updated config file already exists:"
            )
            project_info = '\n\t'.join([
                f"{x['describe']['name']} ({x['id']} - "
                f"created by {x['describe']['createdBy']['user']} on "
                f"{datetime.fromtimestamp(x['describe']['created']/1000).strftime('%Y-%m-%d')}" for x in existing_projects
            ])
            print(project_info)

            project_id = existing_projects[0]['id']
            print(
                "Updated config will be uploaded to existing test project: "
                f"{project_id}"
            )

        return project_id

    @staticmethod
    def write_project_id_to_file(project_id, output_filename):
        """
        Write the project ID to a file for use in the next steps of the
        workflow

        Parameters
        ----------
        project_id : str
            ID of the project to write to file
        output_filename : str
            the name of the txt file to write out with the test project ID in
        """
        with open(output_filename, 'w', encoding='utf8') as out_file:
            out_file.write(project_id)


def main():
    """
    Run main functions for getting or creating a DX test project
    """
    args = parse_args()
    dx_manage = DXManage(args)
    config_name = args.input_config.replace('.json', '')
    project_id = dx_manage.get_or_create_dx_project(config_name, args.run_url)
    dx_manage.write_project_id_to_file(project_id, args.output_filename)


if __name__ == '__main__':
    main()
