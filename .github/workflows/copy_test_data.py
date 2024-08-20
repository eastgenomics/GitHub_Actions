import argparse
import concurrent
import dxpy as dx


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description='Information required to copy data'
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
        '-r',
        '--run_id',
        required=True,
        type=str,
        help="ID of the GitHub Actions run"
    )

    return parser.parse_args()

class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args

    @staticmethod
    def call_in_parallel(func, items) -> list:
        """
        Calls the given function in parallel using concurrent.futures on
        the given set of items.

        Parameters
        ----------
        func : callable
            function to call on each item
        items : list
            list of items to call function on

        Returns
        -------
        list
            list of responses
        """
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            concurrent_jobs = {
                executor.submit(func, file_id, dest_folder): (file_id, dest_folder) for file_id, dest_folder in items
            }

            for future in concurrent.futures.as_completed(concurrent_jobs):
                # access returned output as each is returned in any order
                try:
                    results.append(future.result())
                except Exception as exc:
                    # catch any errors that might get raised during querying
                    print(
                        f"Error getting data for {concurrent_jobs[future]}: {exc}"
                    )
                    raise exc

        return results

    def get_details_from_batch_job(self):
        """
        Get the project ID from the batch job that was run

        Returns
        -------
        project_id : str
            The project ID from the batch job
            e.g. 'project-GpfGVP84fj3VgYzj4gf6F6Bq'
        dias_single_folder : str
            name of the dias single folder that was given as input
            e.g. '/output/TWE-240802_1153'

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
            "successfully. Please check this job and instead provide an "
            "eggd_dias_batch job within a 002 project for the relevant assay "
            "which did complete successfully"
        )

        # Get project ID from job details and the single output dir which
        # was given as an input
        project_id = job_details.get('project')
        job_inputs = job_details.get('input')
        dias_single_folder = job_inputs.get('single_output_dir')

        return project_id, dias_single_folder

    def copy_whole_folder_to_project(
        self, source_project, folder_name
    ):
        """
        Clone whole folder from a project over to the 004 test project.
        This is required because eggd_artemis requires files to be in the same
        project.

        Parameters
        ----------
        source_project : str
            DX ID of the project to copy data from
        folder_name : str
            name of the folder to copy data from in the source project

        Returns
        -------
        github_actions_folder: str
            name of the GitHub Actions folder which has already been created
            by the GitHub Actions run
        existing_copied_files: list
            list with IDs of any files which already existed in the project
        """
        # Get just the main folder name without the subfolder, e.g
        # '/output/TWE-240802_1153' -> '/output'
        # as we want to copy both folders (not just the subfolder)
        main_folder = '/'.join(folder_name.split('/')[:-1])
        github_actions_folder = f'/GitHub_Actions_run-{self.args.run_id}'

        # Clone the whole Dias single folder from the 002 project over to the
        # test project in the relevant folder for the GitHub Actions run
        clone_response = dx.api.project_clone(
            source_project,
            input_params={
                'folders': [main_folder],
                'project': self.args.test_project_id,
                'parents': True,
                'destination': github_actions_folder
            }
        )

        existing_copied_files = clone_response.get('exists')

        return github_actions_folder, existing_copied_files

    def find_data_in_original_path(self, project_id, folder_name):
        """
        Find all files in a path in a project. This is used to find all
        files in the Dias single folder in the 002 project, and return the
        original subfolder they were found in

        Parameters
        ----------
        project_id : str
            DX project ID
        folder_name : str
            name of the folder in the project to search in

        Returns
        -------
        original_data : list
            list of files (each as a dict) found in the path in the project
        Example:
        [
            {
                'project': 'project-GpfGVP84fj3VgYzj4gf6F6Bq',
                'id': 'file-GpfQYf04fj3Q01VKbKF777jZ',
                'describe': {
                    'id': 'file-GpfQYf04fj3Q01VKbKF777jZ',
                    'name': '240801_A01303_0431_BHGCFJDRX5_2024-08-02.xlsx',
                    'folder': (
                        '/output/TWE-240802_1153/eggd_artemis/240802_1557'
                    )
                }
            },
            {
                'project': 'project-GpfGVP84fj3VgYzj4gf6F6Bq',
                'id': 'file-GpfJVxQ41P5XfYbVj148b23B',
                'describe': {
                    'id': 'file-GpfJVxQ41P5XfYbVj148b23B',
                    'name': (
                        '131183360-24194R0051-24NGWES20-9526-F-103698'
                        '_markdup_recalibrated_Haplotyper.g.vcf.gz.tbi'
                    ),
                    'folder': '/output/TWE-240802_1153/sentieon-dnaseq-4.2.2'
                }
            }
        ]
        """
        original_data = list(dx.find_data_objects(
            project=project_id,
            folder=folder_name,
            describe={
                'fields': {
                    'name': True,
                    'folder': True
                }
            }
        ))

        return original_data

    @staticmethod
    def prefix_folders_with_github_actions_folder(
        files_in_002_project, existing_files, ga_folder_name
    ):
        """
        Add a prefix with the GitHub Actions folder name to the folder path
        for all files that already exist in the 004 test project so that
        they can be moved to the folder for the run. Using a different GA
        folder for each run prevents errors when the folder has already
        been copied and keeps all inputs for a specific GitHub Actions run in
        the same place

        Parameters
        ----------
        files_in_002_project : list
            list of files (each as a dict) found in the Dias single path
            in the original 002 project
        existing_file_details : list
            list of file IDs for files which already exist in the project
        ga_folder_name : str
            name of the GitHub Actions folder for the run

        Returns
        -------
        file_folder_pairs: list
            list of tuples, each with a file ID and path to move the file to
        Example:
        [
            (
                'file-GpfQYf04fj3Q01VKbKF777jZ',
                (
                    '/GitHub_Actions_run-10467845283/output/TWE-240802_1153/'
                    'eggd_artemis/240802_1557'
                )
            ),
            (
                'file-GpfJVxQ41P5XfYbVj148b23B',
                (
                    '/GitHub_Actions_run-10467845283/output/TWE-240802_1153/'
                    'sentieon-dnaseq-4.2.2'
                )
            )
        ]
        """
        existing_file_details = [
            file for file in files_in_002_project
            if file['id'] in existing_files
        ]

        # Add the GitHub Actions run folder to the beginning of each original
        # path so any files already existing in the project can be moved
        # to the correct GA folder for this run.
        # This is done because if you try and just clone the Dias single
        # folder into an /output/ dir and there are files already there, it
        # will error
        modified_folders = [{
            **item, 'describe': {
                **item['describe'],
                'folder': ga_folder_name + item['describe']['folder']
            }
        } for item in existing_file_details]

        # Get the file ID and the new folder path for each file as tuples
        file_folder_pairs = [
            (file['id'], file['describe']['folder'])
            for file in modified_folders
        ]

        return file_folder_pairs

    def move_one_file(self, file_id, destination_folder):
        """
        Move one file to a specific folder in the 004 test project

        Parameters
        ----------
        file_id : str
            file ID of the object in format file-XXXX
        destination_folder : str
            name of the folder to move the file to
        """

        file_object = dx.DXFile(
            dxid=file_id,
            project=self.args.test_project_id
        )

        file_object.move(
            folder=destination_folder
        )

        print(f"Moved {file_id} to {destination_folder}")

    def move_already_copied_files_to_correct_folder(
        self, existing_file_folder_pairs
    ):
        """
        Move all files which are already present in the test project to
        the correct folder for the GitHub Actions run, in parallel

        Parameters
        ----------
        existing_file_folder_pairs : list
            list of tuples, each with a file ID and path to move the file to
        """
        self.call_in_parallel(
            self.move_one_file, existing_file_folder_pairs
        )

    @staticmethod
    def find_qc_status_file_in_project(project_id):
        """
        Find the QC status file in a project, which is manually created and
        uploaded to the 002 project for a run

        Parameters
        ----------
        project_id : str
            DX ID of the project to search in

        Returns
        -------
        qc_status_id : str
            DX ID of the QC status file

        Raises
        ------
        AssertionError
            When multiple QC status files are found in the project
        """
        # Find the QC status file, which should be named
        # '<run>_QC_Status.xlsx' - this search is case insensitive so will
        # pick up e.g. _qc_status.xlsx or QC_status.xlsx
        qc_status_file = list(dx.find_data_objects(
            project=project_id,
            name='(?i)qc_status\.xlsx$',
            name_mode='regexp'
        ))

        if qc_status_file:
            assert len(qc_status_file) == 1, (
                "Error: Multiple QC status file(s) found in project"
            )
            qc_status_id = (
                f"{qc_status_file[0]['project']}:{qc_status_file[0]['id']}"
            )
        else:
            qc_status_id = None

        return qc_status_id

    def copy_file_to_test_project_folder(self, file_id, folder_name):
        """
        Copy a file to a specific folder in the 004 test project

        Parameters
        ----------
        file_id : str
            DX file ID of the file to be copied
        folder_name : str
            name of the folder to copy the file into
        """
        # Create DXFile object for file in original project
        source_project, file_id = file_id.split(':')
        file_obj = dx.DXFile(file_id, project=source_project)

        # Copy file over to folder in another project
        file_obj.clone(
            self.args.test_project_id,
            folder=folder_name
        )


def main():
    """
    Run main functions for setting off testing jobs
    """
    args = parse_args()
    dx_manage = DXManage(args)
    # Get inputs from eggd_dias_batch job given in repository variable
    source_project_id, single_folder = dx_manage.get_details_from_batch_job()
    # Copy the whole Dias single folder from the 002 project to the 004 test
    # project so files are in the right project for eggd_artemis. This will
    # return any file IDs which already exist
    ga_folder, existing_files = dx_manage.copy_whole_folder_to_project(
        source_project=source_project_id,
        folder_name=single_folder
    )
    # Find all files in the original dias single path in the 002 project
    dias_files_in_original_project = dx_manage.find_data_in_original_path(
        project_id=source_project_id,
        folder_name=single_folder
    )
    # Get a list of files which need to be moved in the 004 test project
    # to the correct folder for the GitHub Actions run and move them
    file_folder_pairs = dx_manage.prefix_folders_with_github_actions_folder(
        files_in_002_project=dias_files_in_original_project,
        existing_files=existing_files,
        ga_folder_name=ga_folder
    )
    dx_manage.move_already_copied_files_to_correct_folder(
        file_folder_pairs
    )

    # QC status report is uploaded manually by user and so is in the root
    # of the 002 project, not the Dias single folder
    # Find if the QC status file is already in the 004 test project
    # (i.e. it was copied over in a previous GitHub Actions run)
    qc_status_id = dx_manage.find_qc_status_file_in_project(
        args.test_project_id
    )
    # If there is, move it to the correct folder in the same 004 project
    if qc_status_id:
        _, file_id = qc_status_id.split(':')
        dx_manage.move_one_file(file_id, ga_folder)
    # Otherwise find the QC status file in original 002 project and copy it
    # to the correct folder in our 004 test project
    else:
        qc_status = dx_manage.find_qc_status_file_in_project(source_project_id)
        assert qc_status, (
            "Error: No QC status file found in original project "
            f"{source_project_id}"
        )
        dx_manage.copy_file_to_test_project_folder(qc_status, ga_folder)

if __name__ == '__main__':
    main()
