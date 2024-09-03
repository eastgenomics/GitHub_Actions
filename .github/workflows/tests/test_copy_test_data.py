import concurrent
import dxpy as dx
import pytest
import re
import unittest

from unittest.mock import patch, MagicMock

from copy_test_data import DXManage


@patch(
    'copy_test_data.concurrent.futures.ThreadPoolExecutor.submit',
    wraps=concurrent.futures.ThreadPoolExecutor().submit
)
@patch('copy_test_data.DXManage.move_one_file')
class TestCallInParallel(unittest.TestCase):
    """
    Test function DXManage().call_in_parallel which takes a function
    and inputs and calls the function in parallel
    """
    def setUp(self):
        # Set up a default mock args object to make class instance
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    def test_call_in_parallel(self, mock_move, mock_submit):
        """
        Test function called correct number of times, once for each of
        the two inputs
        """
        self.dx_manage.call_in_parallel(
            mock_move,
            items=[('file-xx', 'folder-1'), ('file-yy', 'folder-2')]
        )

        assert mock_submit.call_count == 2, (
            'Function not called correct number of times when '
            'moving files concurrently'
        )

    def test_exceptions_caught_and_raised(self, mock_move, mock_submit):
        """
        Test that if one of the moves raises an Exception that this
        is caught and raised
        """
        # raise error one out of 4 of the _find calls
        mock_move.side_effect = [
            ['foo'],
            ['bar'],
            dx.exceptions.DXError  # generic dxpy error
        ]

        with pytest.raises(dx.exceptions.DXError):
            self.dx_manage.call_in_parallel(
                mock_move,
                items=[
                    ('file-xx', 'folder-1'),
                    ('file-yy', 'folder-2'),
                    ('file-zz', 'folder-3')
                ]
            )


class GetDetailsFromBatchJob(unittest.TestCase):
    """
    Tests function DXManage().get_details_from_batch_job() which gets the
    002 project ID and the input Dias single folder from an eggd_dias_batch
    job ID
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.assay_job_id = 'job-1234'
        self.dx_manage = DXManage(self.mock_args)

    @patch('copy_test_data.dx.describe')
    def test_get_job_output_details(self, mock_describe):
        """
        Test getting output details correctly
        """
        # mock minimal describe output with keys we use
        mock_describe.return_value = {
            'id': 'job-1234',
            'project': 'project-5678',
            'state': 'done',
            'input': {
                'single_output_dir': '/output/TWE-240802_1153'
            }
        }

        result = self.dx_manage.get_details_from_batch_job()

        expected_output = ('project-5678', '/output/TWE-240802_1153')

        assert result == expected_output, (
            'Project ID and Dias single folder not returned as expected'
        )

    @patch('copy_test_data.dx.describe')
    def test_error_raised_if_describe_fails(self, mock_describe):
        """
        Test an error is raised if the describe method fails (because the
        job ID is not valid)
        """
        self.dx_manage.args.assay_job_id = 'job-invalid-id'

        mock_describe.side_effect = dx.exceptions.DXError()

        expected_error = (
            "Cannot call dx describe on job ID job-invalid-id "
            "given as prod job:\n"
        )

        with pytest.raises(dx.exceptions.DXError, match=expected_error):
            self.dx_manage.get_details_from_batch_job()

    @patch('copy_test_data.dx.describe')
    def test_error_raised_if_state_not_done(self, mock_describe):
        """
        Test error is raised if the job which is provided (as a GitHub Actions
        repository variable) does not have a state of 'done'
        """
        # mock minimal describe output with keys we use with state not done
        mock_describe.return_value = {
            'id': 'job-1234',
            'project': 'project-5678',
            'state': 'failed',
            'input': {
                'single_output_dir': '/output/TWE-240802_1153'
            }
        }

        expected_error = re.escape(
            "The production job ID given (job-1234) for "
            "the assay as a repository variable did not complete "
            "successfully. Please check this job and instead provide an "
            "eggd_dias_batch job within a 002 project for the relevant assay "
            "which did complete successfully"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_details_from_batch_job()


class TestCopyWholeFolderToProject(unittest.TestCase):
    """
    Tests for DXManage().copy_whole_folder_to_project function which
    copies a folder (and subfolders and data within) from one project
    to another
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.test_project_id = 'project-564'
        self.mock_args.folder_name = '/GitHub_Actions-folder'
        self.dx_manage = DXManage(self.mock_args)

    @patch('copy_test_data.dx.api.project_clone')
    def test_existing_files_returned_when_already_copied(
        self, mock_project_clone
    ):
        """
        Test files which exist in the destination folder are returned
        correctly when they have already been copied previously
        """
        mock_project_clone.return_value = {
            'exists': ['file-123', 'file-345']
        }

        # Set up args to function
        source_project = 'project-5678'
        copy_folder_name = '/output/TWE-240802_1153'

        # Call function
        result = self.dx_manage.copy_whole_folder_to_project(
            source_project,
            copy_folder_name
        )

        with self.subTest('Test existing files returned as expected'):
            expected_result = ['file-123', 'file-345']

            assert result == expected_result, (
                'Existing files not returned as expected'
            )

        with self.subTest('Check project_clone called with correct args'):
            mock_project_clone.assert_called_once_with(
                'project-5678',
                input_params={
                    'folders': ['/output'],
                    'project': 'project-564',
                    'parents': True,
                    'destination': '/GitHub_Actions-folder'
                }
            )

    @patch('copy_test_data.dx.api.project_clone')
    def test_no_existing_files_returned_correctly(self, mock_project_clone):
        """
        Test empty list returned when there are no files trying to be copied
        over which already exist in the destination project
        """
        mock_project_clone.return_value = {
            'exists': []
        }

        # Set up args to function
        source_project = 'project-5678'
        copy_folder_name = '/output/TWE-240802_1153'

        # Call function
        result = self.dx_manage.copy_whole_folder_to_project(
            source_project,
            copy_folder_name
        )

        with self.subTest('Test no existing files returned'):
            expected_result = []

            assert result == expected_result, (
                'Empty list not returned when no files already exist'
                ' in destination test project'
            )


class TestFindDataInOriginalPath(unittest.TestCase):
    """
    Test DXManage().find_data_in_original_path function which finds all
    files in a folder in a DNAnexus project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @patch('copy_test_data.dx.find_data_objects')
    def test_find_data_in_original_path_when_exists(self, mock_find):
        """
        Test that data returned as expected when found in DNAnexus
        """
        mock_find.return_value = [
            {
                'project': 'project-123',
                'id': 'file-1234',
                'describe': {
                    'name': 'a_file_1.xlsx',
                    'folder': '/output/TWE-240823_1107/eggd_artemis'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-5678',
                'describe': {
                    'name': 'a_file_2.vcf.gz',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            }
        ]

        data_in_original_path = self.dx_manage.find_data_in_original_path(
            'project-123', '/output/TWE-240823_1107/'
        )

        expected_output = [
            {
                'project': 'project-123',
                'id': 'file-1234',
                'describe': {
                    'name': 'a_file_1.xlsx',
                    'folder': '/output/TWE-240823_1107/eggd_artemis'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-5678',
                'describe': {
                    'name': 'a_file_2.vcf.gz',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            }
        ]

        assert data_in_original_path == expected_output, (
            "Data not returned as expected when found in original path"
        )

    @patch('copy_test_data.dx.find_data_objects')
    def test_error_raised_when_no_data_found(self, mock_find):
        """
        Test that error raised when no data is found
        """
        mock_find.return_value = []

        expected_error = (
            "No files were found in the original Dias single path "
            "/output/TWE-240823_1107/ in the 002 project project-123. "
            "Check if these have been moved/deleted"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.find_data_in_original_path(
                'project-123', '/output/TWE-240823_1107/'
            )


class TestPrefixFoldersWithGitHubActionsFolder(unittest.TestCase):
    """
    Test function DXManage().prefix_folders_with_github_actions_folder
    which adds the name of the GA folder which we've created as part of
    the workflow run to the beginning of the path so that we copy files
    into the correct folder in the test project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.folder_name = '/GitHub_Actions-run123_240823_1234'
        self.dx_manage = DXManage(self.mock_args)

    def test_folders_prefixed_correctly_when_files_already_copied(self):
        """
        Test folders are prefixed correctly with GitHub Actions folder name
        """
        files_in_002_project = [
            {
                'project': 'project-123',
                'id': 'file-1234',
                'describe': {
                    'name': 'a_file_1.xlsx',
                    'folder': '/output/TWE-240823_1107/eggd_artemis'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-5678',
                'describe': {
                    'name': 'a_file_2.vcf.gz',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-9101',
                'describe': {
                    'name': 'a_file_3.bam',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            }
        ]

        existing_files = [
            'file-1234',
            'file-5678',
            'file-9101'
        ]
        modified_folders = (
            self.dx_manage.prefix_folders_with_github_actions_folder(
                files_in_002_project, existing_files
            )
        )

        expected_output = [
            (
                'file-1234', (
                    '/GitHub_Actions-run123_240823_1234/output/'
                    'TWE-240823_1107/eggd_artemis'
                )
            ),
            (
                'file-5678', (
                    '/GitHub_Actions-run123_240823_1234/output/'
                    'TWE-240823_1107/sentieon-dnaseq'
                )
            ),
            (
                'file-9101', (
                    '/GitHub_Actions-run123_240823_1234/output/'
                    'TWE-240823_1107/sentieon-dnaseq'
                )
            )
        ]

        assert modified_folders == expected_output, (
            "Folders not prefixed correctly when files exist in the project"
        )

    def test_no_pairs_if_no_files_are_already_copied(self):
        """
        Test an empty list with no file pairs is returned if no files
        which have been copied already exist in the project
        """
        files_in_002_project = [
            {
                'project': 'project-123',
                'id': 'file-1234',
                'describe': {
                    'name': 'a_file_1.xlsx',
                    'folder': '/output/TWE-240823_1107/eggd_artemis'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-5678',
                'describe': {
                    'name': 'a_file_2.vcf.gz',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-9101',
                'describe': {
                    'name': 'a_file_3.bam',
                    'folder': '/output/TWE-240823_1107/sentieon-dnaseq'
                }
            }
        ]
        existing_files = []

        expected_output = []

        modified_folders = (
            self.dx_manage.prefix_folders_with_github_actions_folder(
                files_in_002_project, existing_files
            )
        )

        assert modified_folders == expected_output, (
            "Folders not prefixed correctly when no existing files in project"
        )

    def test_if_no_files_in_002_project_and_none_copied(self):
        """
        Test an empty list with no file pairs is returned if no files
        which have been copied already exist in the project and no files
        were found in the original 002 project
        """
        files_in_002_project = []
        existing_files = []

        expected_output = []

        modified_folders = (
            self.dx_manage.prefix_folders_with_github_actions_folder(
                files_in_002_project, existing_files
            )
        )

        assert modified_folders == expected_output, (
            "Folders not prefixed correctly when no files exist in project"
            " and no files were found in the original 002 project"
        )


class TestMoveOneFile(unittest.TestCase):
    """
    Test function DXManage().move_one_file which moves a single file to
    another folder within the same project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.test_project_id = 'project-896'
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    @patch('copy_test_data.dx.DXFile')
    def test_move_one_file(self, mock_file):
        """
        Test moving file
        """
        # Set up mock DXFile object and methods
        mock_file_obj = MagicMock()
        mock_file.return_value = mock_file_obj

        # Set up args to function
        file_id = 'file-abcde'
        destination_folder = 'folder-1'

        # Call function
        self.dx_manage.move_one_file(
            file_id,
            destination_folder
        )

        with self.subTest('File object initialised with correct args'):
            mock_file.assert_called_once_with(
                dxid='file-abcde',
                project='project-896'
            )

        with self.subTest('Move method called with correct dest folder'):
            mock_file_obj.move.assert_called_once_with(
                folder=destination_folder
            )

        with self.subTest('Moved file printed as expected'):
            stdout = self.capsys.readouterr().out

            expected_print = "Moved file-abcde to folder-1"

            assert expected_print in stdout, (
                "Info on file moved and folder info not printed as expected"
            )

@patch('copy_test_data.DXManage.call_in_parallel')
@patch('copy_test_data.DXManage.move_one_file')
class TestMoveAlreadyCopiedFilesToCorrectFolder(unittest.TestCase):
    """
    Test function DXManage().move_already_copied_files_to_correct_folder()
    which moves files to folders concurrently
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    def test_moving_files_in_parallel(self, mock_move, mock_parallel):
        """
        Test moving files in parallel called with correct args
        """
        self.dx_manage.move_already_copied_files_to_correct_folder(
            [
                ('file-xx', 'folder-1'),
                ('file-yy', 'folder-2')
            ]
        )

        mock_parallel.assert_called_once_with(
            mock_move,
            [
                ('file-xx', 'folder-1'),
                ('file-yy', 'folder-2')
            ]
        )


class TestFindQCStatusFileInProject(unittest.TestCase):
    """
    Tests finding of QC status file in DNAnexus project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @patch('copy_test_data.dx.find_data_objects')
    def test_qc_status_id_returned_correctly(self, mock_find):
        """
        Test QC status file ID is returned correctly
        """
        # mock minimal DX find response
        mock_find.return_value = [
            {
                'project': 'project-1234',
                'name': 'qc_status.xlsx',
                'id': 'file-1234'
            }
        ]

        qc_status_id = self.dx_manage.find_qc_status_file_in_project(
            'project-1234'
        )

        assert qc_status_id == 'project-1234:file-1234', (
            "QC status ID not returned correctly"
        )

    @patch('copy_test_data.dx.find_data_objects')
    def test_error_raised_when_multiple_qc_files_found(self, mock_find):
        """
        Test error is raised if there are multiple QC status files found
        in a project
        """
        mock_find.return_value = [
            {
                'project': 'project-1234',
                'name': 'qc_status.xlsx',
                'id': 'file-1234'
            },
            {
                'project': 'project-1234',
                'name': 'another_qc_status.xlsx',
                'id': 'file-5678'
            }
        ]

        expected_error = (
            "Error: Multiple QC status file\\(s\\) found in project"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.find_qc_status_file_in_project('project-1234')

    @patch('copy_test_data.dx.find_data_objects')
    def test_none_returned_when_no_qc_status_found(self, mock_find):
        """
        Test None is returned if no QC status file is found in a project
        """
        mock_find.return_value = []

        qc_status_id = self.dx_manage.find_qc_status_file_in_project(
            'project-1234'
        )

        assert qc_status_id is None, (
            "QC status ID should be None if no file found"
        )


class TestCopyFileToTestProjectFolder(unittest.TestCase):
    """
    Tests for function DXManage().copy_file_to_test_project_folder
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.test_project_id = 'project-1234'
        self.mock_args.folder_name = '/GitHub_Actions-run123_240823_1234'
        self.dx_manage = DXManage(self.mock_args)

    @patch('copy_test_data.dx.DXFile')
    def test_copy_file_to_project_folder(self, mock_file):
        """
        Test copying one file to a project into a folder
        """
        # Set up mock DXFile object and methods
        mock_file_obj = MagicMock()
        mock_file.return_value = mock_file_obj

        file_id = 'project-345:file-1234'

        self.dx_manage.copy_file_to_test_project_folder(file_id)

        # Assert each method called once with correct args
        with self.subTest('File object called once with correct args'):
            mock_file.assert_called_once_with(
                'file-1234',
                project='project-345'
            )

        with self.subTest('File clone called once with correct inputs'):
            mock_file_obj.clone.assert_called_once_with(
                'project-1234',
                folder='/GitHub_Actions-run123_240823_1234'
            )
