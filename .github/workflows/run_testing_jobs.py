"""
Script which sets off testing jobs in a DNAnexus project
"""

import argparse
import concurrent.futures
import dxpy as dx
import json


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
        type=int,
        default=5,
        help="The number of samples to set off test jobs for"
    )

    parser.add_argument(
        '-i',
        '--run_id',
        required=True,
        type=str,
        help="ID of the GitHub Actions run to name the test output folder"
    )

    parser.add_argument(
        '-o',
        '--out_file',
        required=True,
        type=str,
        help="Name of the file to write the launched job ID to"
    )


    parser.add_argument(
        '-a',
        '--assay',
        required=True,
        type=str,
        choices=['TWE', 'CEN'],
        help=(
            "The assay that the updated config file relates to"
        )
    )

    parser.add_argument(
        '-r',
        '--run_cnv_calling',
        required=True,
        type=bool,
        help='If CEN assay, whether to re-use CNV calling job outputs'
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
        config_info_file : str
            name of JSON file to read in

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

    def get_cnv_calling_job_id(self):
        """
        Get original CNV calling job ID which was launched by the production
        eggd_dias_batch_job

        Returns
        -------
        cnv_calling_jobs : str
            DX job ID of the CNV calling job which was launched by the prod
            eggd_dias_batch job given

        Raises
        ------
        AssertionError
            When more than one GATKgCNV job was launched by the original
            eggd_dias_batch job
        """
        print(
            "Getting CNV calling job ID from original eggd_dias_batch job"
            f" {self.args.job_id}"
        )
        # Describe the job to get the output launched jobs
        job_details = dx.DXJob(dxid=self.args.job_id).describe()

        # Get output launched jobs
        job_output_ids = job_details.get('output').get('launched_jobs')
        launched_list = job_output_ids.split(',')

        # Get just jobs launched (as will include reports analyses)
        launched_jobs = [
            job for job in launched_list if job.startswith('job-')
        ]

        # Get any jobs with GATKgCNV in the name
        cnv_calling_jobs = [
            job for job in launched_jobs
            if 'GATKgCNV' in dx.describe(job)['name']
        ]

        # Assert we've found one job
        assert len(cnv_calling_jobs) == 1, (
            "Error: No or multiple CNV calling jobs launched by "
            f"eggd_dias_batch job {self.args.job_id} given"
        )

        return cnv_calling_jobs[0]

    def get_project_id_from_batch_job(self):
        """
        Get the project ID from the batch job that was run

        Returns
        -------
        project_id : str
            The project ID from the batch job

        Raises
        ------
        DXError
            When dx describe cannot be called on job ID given (not valid job)
        AssertionError
            When the job ID which is given to re-run testing with did not
            complete successfully
        """
        try:
            job_details = dx.describe(self.args.assay_job_id)
        except dx.exceptions.DXError as err:
            # dx error raised - can't call describe on job ID given
            raise dx.exceptions.DXError(
                f"Cannot call dx describe on job ID {self.args.assay_job_id} "
                f"given as prod job:\n {err}"
            )

        assert job_details.get('state') == 'done', (
            f"The production job ID given ({self.args.assay_job_id}) for "
            "the assay as a repository variable did not complete "
            "successfully. Please check this job and instead provide a "
            "job within a 002 project for the relevant assay which did "
            "complete successfully"
        )

        project_id = job_details.get('project')

        return project_id

    def find_all_executions_in_project(self):
        """
        Find all executions (jobs/analyses) in the test 004 project

        Returns
        -------
        executions : list
            list of dicts, each containing information about a job/analysis
        """
        executions = list(dx.find_executions(
            project=self.args.test_project_id,
            describe={
                'fields': {
                    'state': True
                }
            }
        ))

        return executions

    @staticmethod
    def find_non_terminal_jobs(executions):
        """
        Find any non-complete/failed/terminated jobs to be terminated

        Parameters
        ----------
        executions : list
            list, where if jobs, is a list of dicts with each containing
            information about a job/analysis

        Returns
        -------
        non_terminal_job_ids: list
            list with IDs of any jobs/analyses to terminate
        """
        end_states = ['done', 'terminated', 'failed']

        # Get any jobs with non-end states
        if executions:
            non_terminal_job_ids = [
                job['id'] for job in executions
                if job['describe']['state'] not in end_states
            ]

            print("Running jobs will be terminated:")
            print('\n'.join([
                f"{job} - {dx.describe(job)['describe']['name']}"
                for job in non_terminal_job_ids
            ]))
        else:
            non_terminal_job_ids = []

        return non_terminal_job_ids

    @staticmethod
    def terminate(jobs):
        """
        Terminate all jobs based on a list of job/analysis IDs given

        Parameters
        ----------
        jobs : list
            list of job / analysis IDs
        """
        def terminate_one(job) -> None:
            """dx call to terminate single job"""
            if job.startswith('job'):
                dx.DXJob(dxid=job).terminate()
            else:
                dx.DXAnalysis(dxid=job).terminate()

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            concurrent_jobs = {
                executor.submit(terminate_one, id):
                id for id in sorted(jobs, reverse=True)
            }
            for future in concurrent.futures.as_completed(concurrent_jobs):
                # access returned output as each is returned in any order
                try:
                    future.result()
                except Exception as exc:
                    # catch any errors that might get raised
                    print(
                        "Error terminating job "
                        f"{concurrent_jobs[future]}: {exc}"
                    )

        print("Terminated all current jobs.")

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
            f"\n{executable_info}\n. This is not expected - please change the"
            " job for this assay in the repository's GA variables"
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
                " within the repository's GitHub Actions variables"
            )

        return executable_str

    def find_multiqc_report_in_proj(self, project_id):
        """
        Find the file ID of the MultiQC report to use as an input for the
        eggd_dias_batch test job we set off, otherwise the eggd_artemis job
        we set off will fail because no MultiQC job is run in the 004 test
        project

        Parameters
        ----------
        project_id : str
            DX project ID

        Returns
        -------
        multiqc_report_id : str
            DX file ID of the MultiQC report in format project-id:file-id

        Raises
        ------
        AssertionError
            When no or multiple MultiQC reports are found in the original 002
            project
        """
        # Find MultiQC reports by suffix in the original 002 project
        multiqc_reports = list(dx.find_data_objects(
            project=project_id,
            name='*multiqc*html',
            name_mode='glob'
        ))

        assert len(multiqc_reports) == 1, (
            "Error: No or multiple MultiQC report(s) found in project"
            f" {project_id}"
        )

        # Format as project-id:file-id
        multiqc_report_id = (
            f"{multiqc_reports[0]['project']}:{multiqc_reports[0]['id']}"
        )

        return multiqc_report_id

    def get_actions_folder(self):
        """
        Get the folder that was created in the test project to put the
        output of the test jobs into

        Returns
        -------
        folder_name : str
            name of the folder which was created in an earlier step which
            contains the GitHub Actions run ID

        Raises
        ------
        AssertionError
            When no or multiple folders are found in the test project which
            relate to this GitHub Actions run
        """
        # Find folders in the test 004 project
        folders_list = dx.api.project_list_folder(
            object_id=self.args.test_project_id,
            input_params={
                'only': 'folders'
            }
        )

        # Get name(s) of folders which have the GitHub Actions run ID in the
        # name -> this should only be one. If no folders in project, this will
        # be an empty list
        folder_names = [
            folder for folder in folders_list.get('folders') if self.args.run_id in folder
        ]

        assert len(folder_names) == 1, (
            "Error: No or multiple folder(s) found for this GitHub Actions run"
            f" in the test project {self.args.test_project_id}"
        )

        folder_name = folder_names[0]

        return folder_name

    def set_off_test_jobs(
        self, updated_config_id, folder_name, multiqc_report, cnv_job_id
    ) -> str:
        """
        Set off the job in the test project using inputs from
        the original job but with the updated config file instead

        Parameters
        ----------
        updated_config_id : str
            DX file ID of the updated config file
        folder_name: str
            name of the DX folder in the test project to save outputs to
        multiqc_report : str
            DX file ID of the MultiQC report in format project-id:file-id
        cnv_job_id: str
            DX job ID of CNV calling job to re-use outputs from

        Returns
        -------
        job_id : str
            DX job ID of the test job that has been set off
        """
        # Describe the original job to get the inputs
        job_details = dx.describe(self.args.assay_job_id)
        job_inputs = job_details.get('input')
        app_name = job_details.get('executableName')

        # Get the project the original job was run in so that we can
        # provide the full project:/folder path and re-run the original
        # job in our test project
        original_single_path = job_inputs.get('single_output_dir')

        # Replace some inputs to test our config file
        _, multi_file_id = multiqc_report.split(':')
        job_inputs['single_output_dir'] = (
            f'{folder_name}{original_single_path}'
        )
        job_inputs['assay_config_file'] = {
            '$dnanexus_link': updated_config_id
        }
        job_inputs['multiqc_report'] = {
            '$dnanexus_link': multi_file_id
        }

        # Only set off jobs for a subset of samples - this is set by a
        # repository GitHub Actions variable
        job_inputs['sample_limit'] = self.args.test_sample_limit

        # If CNV calling job ID given - re-use the job outputs - set by
        # GitHub Actions repository variable
        if cnv_job_id:
            job_inputs['cnv_call_job_id'] = cnv_job_id

        print("Setting off job in test project with updated config file:\n")
        prettier_print(job_inputs)

        # Set off job in the test project with our newly created output folder
        job = dx.DXApp(name=app_name).run(
            app_input=job_inputs,
            project=self.args.test_project_id,
            folder=folder_name
        )

        # Add tag to the job with the GitHub Actions run ID
        job_id = job.describe().get('id')
        job_handle = dx.DXJob(dxid=job_id)
        job_handle.add_tags(
            tags=[f'GitHub Actions run ID: {self.args.run_id}']
        )

        return job_id

    def write_out_job_id(self, job_id, outfile_name):
        """
        Write out a file with the ID of the job which has been launched

        Parameters
        ----------
        job_id : str
            DX ID of the job which was launched
        outfile_name : str
            name of the output txt file
        """
        with open(outfile_name, 'w', encoding='utf8') as out_file:
            out_file.write(job_id)


def main():
    """
    Run main functions for setting off testing jobs
    """
    args = parse_args()
    dx_manage = DXManage(args)
    config_info = dx_manage.read_in_json(args.config_info)
    updated_config_id = config_info.get('updated').get('dxid')
    project_id = dx_manage.get_project_id_from_batch_job()

    # Terminate any jobs running in test project as these would be from
    # previous GA run - no longer relevant if changes have been made
    executions = dx_manage.find_all_executions_in_project()
    non_terminal_executions = dx_manage.find_non_terminal_jobs(executions)
    if non_terminal_executions:
        dx_manage.terminate(non_terminal_executions)

    # Find MultiQC report in 004 project to use as input for test job
    # since otherwise eggd_artemis automatically finds the MultiQC report via
    # a job in the 004 project (and this does not exist)
    multiqc_report = dx_manage.find_multiqc_report_in_proj(project_id)

    # Check dias_single version used in 002 project and set off test jobs
    dx_manage.check_dias_single_version_which_generated_data(project_id)

    # If CEN assay and variable in repo is set to not run CNV calling,
    # get CNV calling job ID from prod CEN job given
    if args.assay == 'CEN' and not args.run_cnv_calling:
        cnv_calling_job_id = dx_manage.get_cnv_calling_job_id()
    else:
        cnv_calling_job_id = None

    # Set off test job with the updated config based on the prod job given
    # for the relevant assay
    folder_name = dx_manage.get_actions_folder()
    job_id = dx_manage.set_off_test_jobs(
        updated_config_id,
        folder_name,
        multiqc_report,
        cnv_calling_job_id
    )
    dx_manage.write_out_job_id(job_id, args.out_file)


if __name__ == '__main__':
    main()
