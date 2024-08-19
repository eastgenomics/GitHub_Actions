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
        AssertionError
            When the job ID which is given to re-run testing with did not
            complete successfully
        """
        job_details = dx.describe(self.args.assay_job_id)
        project_id = job_details.get('project')

        assert project_id, (
            "Couldn't parse project ID from the job ID given. Please check"
            " that this is a valid eggd_dias_batch job ID"
        )

        assert job_details.get('state') == 'done', (
            f"The production job ID given ({self.args.assay_job_id}) for "
            "the assay as a repository variable did not complete "
            "successfully. Please check this job and instead provide a "
            "job within a 002 project for the relevant assay which did "
            "complete successfully"
        )

        return project_id

    def get_dias_single_path_from_prod_job(self):
        """
        Get the path to the Dias single folder in the original 002 job

        Returns
        -------
        _type_
            _description_
        """
        # Describe the original job to get the inputs
        job_details = dx.describe(self.args.assay_job_id)
        job_inputs = job_details.get('input')

        # Get the project the original job was run in so that we can
        # provide the full project:/folder path and re-run the original
        # job in our test project
        dias_single_folder = job_inputs.get('single_output_dir')

        return dias_single_folder

    def copy_whole_folder_to_project(
        self, source_project, dias_single_folder
    ):
        """
        Clone whole folder from a project over to another project (because
        artemis requires files to be in the same
        project)

        Parameters
        ----------
        source_project : _type_
            _description_
        dias_single_folder : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        main_folder = '/'.join(dias_single_folder.split('/')[:-1])
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

    def find_data_in_original_path(self, project_id, dias_single_folder):
        """
        Find all files in the Dias single path in the original 002 project

        Parameters
        ----------
        project_id : _type_
            _description_
        dias_single_folder : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        original_data = list(dx.find_data_objects(
            project=project_id,
            folder=dias_single_folder,
            describe={
                'fields': {
                    'name': True,
                    'folder': True
                }
            }
        ))

        return original_data

    @staticmethod
    def prefix_folder_path_with_github_actions_folder(
        existing_files, ga_folder_name, files_in_002_project
    ):
        """
        _summary_

        Parameters
        ----------
        existing_file_details : _type_
            _description_
        ga_folder_name : _type_
            _description_

        Returns
        -------
        _type_
            _description_
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
        Move one file to a specific folder

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
        _summary_

        Parameters
        ----------
        existing_copied_files : _type_
            _description_
        """

        self.call_in_parallel(
            self.move_one_file, existing_file_folder_pairs
        )

    @staticmethod
    def find_qc_status_file_in_project(project_id):
        """
        Find the QC status file in a project
        """
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
    source_project_id = dx_manage.get_project_id_from_batch_job()
    single_folder = dx_manage.get_dias_single_path_from_prod_job()
    ga_folder, existing_files = dx_manage.copy_whole_folder_to_project(
        source_project=source_project_id,
        dias_single_folder=single_folder
    )
    dias_files_in_original_project = dx_manage.find_data_in_original_path(
        project_id=source_project_id,
        dias_single_folder=single_folder
    )
    file_folder_pairs = dx_manage.prefix_folder_path_with_github_actions_folder(
        existing_files=existing_files,
        ga_folder_name=ga_folder,
        files_in_002_project=dias_files_in_original_project
    )
    dx_manage.move_already_copied_files_to_correct_folder(
        file_folder_pairs
    )

    # QC status report is uploaded manually by user and so is in the root
    # of the 002 project, not the Dias single folder
    # Find if there is already the QC status file in the 004 test project
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
