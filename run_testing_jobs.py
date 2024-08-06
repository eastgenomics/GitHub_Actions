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
    parser = argparse.ArgumentParser(
        description='Information required to run testing jobs'
    )

    parser.add_argument(
        '-v',
        '--repo_version',
        required=True,
        type=str,
        help="The current release version of Dias single in GitHub"
    )

    parser.add_argument(
        '-c',
        '--config_info',
        required=True,
        type=str,
        help="JSON file containing info about changed and prod configs"
    )

    parser.add_argument(
        '-t',
        '--test_project_id',
        required=True,
        type=str,
        help="ID of the DX project to run tests in"
    )

    parser.add_argument(
        '-j',
        '--assay_job_id',
        required=True,
        type=str,
        help=(
            "ID of the eggd_dias_batch job that we will be re-using inputs "
            "from"
        )
    )

    parser.add_argument(
        '-l',
        '--test_sample_limit',
        type=str,
        default=5,
        help="The number of samples to set off test jobs for"
    )

    parser.add_argument(
        '-r',
        '--run_id',
        required=True,
        type=str,
        help="ID of the GitHub Actions run to name the test output folder"
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

    def get_project_id_from_batch_job(self):
        """
        Get the project ID from the batch job that was run

        Returns
        -------
        project_id : str
            The project ID from the batch job

        Raises
        ------
        AssertionError
            When the project ID cannot be parsed from the job ID given
        """
        job_details = dx.describe(self.args.assay_job_id)
        project_id = job_details.get('project')

        assert project_id, (
            "Couldn't parse project ID from the job ID given. Please check"
            " that this is a valid eggd_dias_batch job ID"
        )

        return project_id

    def check_dias_single_version_which_generated_data(self, project_id):
        """
        Get the version of Dias single which was used to generate the data
        in the project provided and check that it is the same as the current
        release to prevent us from running tests on data which isn't
        representative of our current production workflows

        Parameters
        ----------
        project_id : str
            DX project ID

        Returns
        -------
        executable_str : str
            the version of Dias single used, e.g. v2.4.0

        Raises
        ------
        AssertionError
            When multiple versions (or no version) of dias single was used
            to generate the data in the project
        """
        dias_single_analyses = list(dx.find_analyses(
            project=project_id,
            name='dias_single_*',
            name_mode='glob',
            describe={
                'fields': {
                    'executableName': True
                }
            }
        ))

        # Get the unique versions of dias single used to generate all data
        # in the project
        executable_versions = list(set([
            analysis['describe']['executableName'].replace('dias_single_', '')
            for analysis in dias_single_analyses
        ]))

        executable_info = '\n\t'.join(
            [version for version in executable_versions]
        )

        # Assert only one version of dias single was used
        assert len(executable_versions) == 1, (
            f"Error: {len(executable_versions)} versions of the dias "
            "single workflow were used to generate data in this project:"
            f"\n{executable_info}\n. This is not expected"
        )

        executable_str = executable_versions[0]
        # Assert the version of dias single used matches the current release
        if executable_str != self.args.repo_version:
            print(
                "Warning: The version of Dias single used to generate data in "
                f"this project ({executable_str}) does not match the current "
                "release version for the Dias single GitHub repo "
                f"({self.args.repo_version}). \nPlease check that this is "
                "expected and if not, update the job ID given for this assay"
            )

        return executable_str

    def create_dx_folder(self):
        """
        Creates a DNAnexus folder for the GitHub Actions run if it does not
        exist

        Returns
        -------
        folder_name : str
            The name of the folder that has been created in the DX test
            project

        Raises
        ------
        RuntimeError
            When a folder for the GitHub Actions run already exists
        """
        # Create a folder to put the job output in
        folder_name = f"/GitHub_Actions_run-{self.args.run_id}"

        try:
            dx.api.project_list_folder(
                object_id=self.args.test_project_id,
                input_params={
                    'folder': folder_name,
                    'only': 'folders'
                },
                always_retry=True
            )

            raise RuntimeError(
                "Error: folder for this GitHub Actions run already exists. "
                "Exiting because this is not expected to happen"
            )

        except dx.exceptions.ResourceNotFound:
            # can't find folder -> create one
            dx.api.project_new_folder(
                object_id=self.args.test_project_id,
                input_params={
                    'folder': folder_name,
                    'parents': True
                }
            )

            prettier_print(f'Created output folder: {folder_name}')

        return folder_name

    def set_off_test_jobs(self, updated_config_id, folder_name) -> str:
        """
        Set off the job in the test project using inputs from
        the original job but with the updated config file instead

        Parameters
        ----------
        updated_config_id : str
            DX file ID of the updated config file
        folder_name: str
            name of the DX folder in the test project to save outputs to

        Returns
        -------
        job_id : str
            DX job ID of the test job that has been set off
        """
        # Describe the original job to get the inputs
        job_details = dx.describe(self.args.assay_job_id)
        job_inputs = job_details.get('input')
        original_proj = job_details.get('project')
        app_name = job_details.get('executableName')

        # Get the project the original job was run in so that we can
        # provide the full project:/folder path and re-run the original
        # job in our test project
        original_single_path = job_inputs.get('single_output_dir')
        path_to_single_with_project = f"{original_proj}:{original_single_path}"

        # Replace some inputs to test our config file
        job_inputs['assay_config_file'] = {
            '$dnanexus_link': updated_config_id
        }
        job_inputs['single_output_dir'] = path_to_single_with_project
        job_inputs['testing'] = True
        job_inputs['sample_limit'] = self.args.test_sample_limit

        # Set off job in the test project with specified output folder
        job = dx.DXApp(name=app_name).run(
            app_input=job_inputs,
            project=self.args.test_project_id,
            folder=folder_name
        )

        # Add tag to the job with the GitHub Actions run number
        job_id = job.describe().get('id')
        job_handle = dx.DXJob(dxid=job_id)
        job_handle.add_tags(
            tags=[f'GitHub Actions run no. {self.args.run_id}']
        )

        return job_id

def main():
    """
    Run main functions for setting off testing jobs
    """
    args = parse_args()
    dx_manage = DXManage(args)
    config_info = dx_manage.read_in_json(args.config_info)
    updated_config_id = config_info.get('updated').get('dxid')
    project_id = dx_manage.get_project_id_from_batch_job()
    dx_manage.check_dias_single_version_which_generated_data(project_id)
    folder_name = dx_manage.create_dx_folder()
    job_id = dx_manage.set_off_test_jobs(
        updated_config_id,
        folder_name
    )


if __name__ == '__main__':
    main()
