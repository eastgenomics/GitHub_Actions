import dxpy as dx
import json
import os
import pytest
import sys
import unittest

from datetime import datetime
from unittest import mock
from unittest.mock import patch, mock_open, MagicMock

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from get_or_create_testing_project import DXManage


class TestFindDXProject(unittest.TestCase):
    """
    Tests finding of DNAnexus projects
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.input_config = 'changed_config.json'
        self.dx_manage = DXManage(self.mock_args)

    @patch('get_or_create_testing_project.dx.find_projects')
    def test_dx_find_project(self, mock_find):
        """
        Test that project returned as expected when mocking return
        """
        mock_find.return_value = [
            {
                'id': 'project-Gpb3k6Q4PZYxVz9pzXvK82Xy',
                'level': 'ADMINISTER',
                'permissionSources': ['user-locker'],
                'public': False,
                'describe': {
                    'id': 'project-Gpb3k6Q4PZYxVz9pzXvK82Xy',
                    'name': (
                        '004_240731_GitHub_Actions_dias_TWE_config_GRCh37_'
                        'v3.1.7_testing'
                    ),
                    'created': 1722432666000,
                    'createdBy': {'user': 'user-locker'}
                }
            }
        ]

        assert self.dx_manage.find_dx_test_project() == (
            mock_find.return_value
        ), "Project not found as expected"

    @patch('get_or_create_testing_project.dx.find_projects')
    def test_none_returned_if_no_projects_found(self, mock_find):
        """
        Test that None returned if no projects found
        """
        mock_find.return_value = []

        assert self.dx_manage.find_dx_test_project() == [], (
            "Empty list not returned when no projects found"
        )

    @patch('get_or_create_testing_project.dx.find_projects')
    def test_error_raised_for_multiple_projects(self, mock_find):
        """
        Test that AssertionError is raised if multiple projects
        named with the config file are found
        """
        mock_find.return_value = [
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
                    'createdBy': {'user': 'user-locker'}
                }
            },
            {
                'id': 'project-XYZ',
                'level': 'ADMINISTER',
                'permissionSources': ['user-locker'],
                'public': False,
                'describe': {
                    'id': 'project-XYZ',
                    'name': '004_240620_GitHub_Actions_dias_TWE_config_GRCh37_v3.1.7_testing',
                    'created': 1722432666000,
                    'createdBy': {'user': 'user-locker'}
                }
            }
        ]

        expected_error = (
            "Found multiple existing 004 testing projects for the updated "
            "config file: \n\tproject-Gpb3k6Q4PZYxVz9pzXvK82Xy: "
            "004_240731_GitHub_Actions_dias_TWE_config_GRCh37_v3.1."
            "7_testing\n\tproject-XYZ: 004_240620_GitHub_Actions"
            "_dias_TWE_config_GRCh37_v3.1.7_testing\n. Please either "
            "rename or delete projects so a maximum of one remains"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.find_dx_test_project()

@patch('get_or_create_testing_project.dx.DXProject')
class TestGetOrCreateDXProject(unittest.TestCase):
    """
    Test function DXManage().get_or_create_dx_test_project() which
    either returns an existing project ID or creates a new one and
    returns that ID
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.input_config = 'changed_config_v1.json'
        self.mock_args.run_url = (
            'https://github.com/eastgenomics/GitHub_Actions/actions/runs/1234'
        )
        self.mock_args.run_id = '1234'
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    def test_id_of_existing_project_returned_when_exists(self, mock_project):
        """
        Check that just the ID of the existing project is returned
        if a list with an existing project is given as input
        """
        existing_projects = [
            {
                'id': 'project-123',
                'level': 'ADMINISTER',
                'permissionSources': ['user-locker'],
                'public': False,
                'describe': {
                    'id': 'project-123',
                    'name': (
                        '004_240731_GitHub_Actions_dias_TWE_config_GRCh37_'
                        'v3.1.7_testing'
                    ),
                    'created': 1722432666000,
                    'createdBy': {'user': 'user-locker'}
                }
            }
        ]

        project_id = self.dx_manage.get_or_create_dx_project(
            existing_projects
        )

        with self.subTest('DXProject.ID returned'):
            assert project_id == 'project-123', (
                "Project ID not returned correctly when project matching the"
                " config file name exists"
            )

        with self.subTest('DXProject.stdout'):
            # Check print statement is as expected when existing project
            stdout = self.capsys.readouterr().out

            expected_print = (
                "Project for testing the updated config file already exists: "
                "\n004_240731_GitHub_Actions_dias_TWE_config_GRCh37_v3.1.7"
                "_testing (project-123) - created by user-locker on "
                "2024-07-31.\nUpdated config will be uploaded to existing"
                " test project project-123\n"
            )

            assert expected_print in stdout, (
                "Info about found project not printed as expected"
            )

        with self.subTest('DXProject.new not called'):
            mock_project.return_value.new.assert_not_called()

        with self.subTest('DXProject.invite not called'):
            mock_project.return_value.invite.assert_not_called()

    @patch('get_or_create_testing_project.datetime')
    def test_new_project_created(self, mock_datetime, mock_dx_project):
        """
        Test that a new project is created and its ID returned when
        no existing project is found
        """
        mock_datetime.now.return_value = datetime(2024, 8, 23)

        # set there to be no existing projects as input
        existing_projects = []
        mock_dx_project.return_value.new.return_value = 'new-project-id'
        mock_dx_project.return_value.invite.return_value = None

        project_id = self.dx_manage.get_or_create_dx_project(
            existing_projects
        )

        with self.subTest('DXProject.ID returned'):
            assert project_id == 'new-project-id', (
                "Project ID not returned correctly when new project created"
            )

        with self.subTest('DXProject.new called with correct inputs'):
            mock_dx_project.return_value.new.assert_called_once_with(
                name='004_240823_GitHub_Actions_changed_config_v1_testing',
                summary='Project for testing changes in changed_config_v1',
                description=(
                    "This project was created automatically by GitHub Actions"
                    ": https://github.com/eastgenomics/GitHub_Actions/"
                    "actions/runs/1234"
                )
            )

        with self.subTest('DXProject.Test new proj print as expected'):
            # Check print statement is as expected for new project
            stdout = self.capsys.readouterr().out

            expected_new_proj_print = (
                "\n004 project for updated config does not exist. Created"
                " new project for testing: 004_240823_GitHub_Actions"
                "_changed_config_v1_testing (new-project-id)"
            )

            assert expected_new_proj_print in stdout, (
                "Info about found project not printed as expected"
            )

            expected_invite_print = (
                "\nGranted CONTRIBUTE privilege to org-emee_1"
            )

            assert expected_invite_print in stdout, (
                "Info about project invite not printed as expected"
            )

        with self.subTest('DXProject.invite'):
            mock_dx_project.return_value.invite.assert_called_once_with(
                'org-emee_1',
                'CONTRIBUTE',
                send_email=False
            )


class TestCreateDXFolder(unittest.TestCase):
    """
    Test function DXManage().create_dx_folder
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.run_id = '12345'
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    @patch("get_or_create_testing_project.dx.api.project_list_folder")
    @patch("get_or_create_testing_project.dx.api.project_new_folder")
    def test_folder_created_when_folder_does_not_exist(
        self, mock_new_folder, mock_list_folder
    ):
        """
        Test that folder name is returned when folder is created
        """
        project = 'project-123'
        timestamp = '240822_1006'

        error_content = {
            "error": {
                "type": "ResourceNotFound",
                "message": (
                    'The specified folder could not be found in project-123'
                )
            }
        }

        # Mock a ResourceNotFound error occurring, weirdly this requires
        # more info than a usual exception
        mock_list_folder.side_effect = dx.exceptions.ResourceNotFound(
            content=error_content,
            code=404,
            timestamp='1724423560.087708'
        )

        folder_name = self.dx_manage.create_dx_folder(
            project, timestamp
        )

        with self.subTest(
            'Folder name returned correctly when does not exist'
        ):
            expected_folder_name = '/GitHub_Actions_run-12345_240822_1006'
            assert expected_folder_name == folder_name, (
                'Folder name not returned correctly when folder does not'
                ' already exist'
            )

        with self.subTest('DXProject.new_folder called with correct inputs'):
            mock_new_folder.assert_called_once_with(
                object_id=project,
                input_params={
                    'folder': '/GitHub_Actions_run-12345_240822_1006',
                    'parents': True
                }
            )

        with self.subTest('DXProject.new_folder print as expected'):
            # Check print statement is as expected for new folder
            stdout = self.capsys.readouterr().out

            expected_new_folder_print = (
                "Created output folder: /GitHub_Actions_run-12345_240822_1006"
            )

            assert expected_new_folder_print in stdout, (
                "Info about found folder not printed as expected"
            )

    @patch("get_or_create_testing_project.dx.api.project_list_folder")
    @patch("get_or_create_testing_project.dx.api.project_new_folder")
    def test_no_folder_created_when_folder_does_exist(
        self, mock_new_folder, mock_list_folder
    ):
        """
        Test that folder name is returned when folder already exists
        """
        project = 'project-123'
        timestamp = '240822_1006'
        mock_list_folder.return_value = {
            'folders': ['/GitHub_Actions_run-12345_240822_1006']
        }
        project = 'project-123'

        expected_folder_name = '/GitHub_Actions_run-12345_240822_1006'

        folder_name = self.dx_manage.create_dx_folder(project, timestamp)

        with self.subTest('Folder name returned correctly'):
            assert expected_folder_name == folder_name, (
                "Folder name returned not correct when folder already exists"
            )

        with self.subTest('DXProject.new_folder not called'):
            mock_new_folder.assert_not_called()


class TestWriteProjectIdToFile(unittest.TestCase):
    def setUp(self):
        # Create a mock object for self.args and set output_filename
        self.mock_args = MagicMock()
        self.mock_args.output_filename = 'dx_project_info.json'
        self.dx_manage = DXManage(self.mock_args)

    @patch('get_or_create_testing_project.open', new_callable=mock_open)
    def test_writing_out_project_id(self, mock_open):
        """
        Test project info written to file correctly
        """
        project_id = 'project-1234'
        folder_name = 'test_folder'

        self.dx_manage.write_project_id_to_file(
            project_id, folder_name
        )

        with self.subTest('File opened correctly'):
            # Check that open() was called with the correct filename
            mock_open.assert_called_once_with(
                'dx_project_info.json', 'w', encoding='utf8'
            )

        with self.subTest('File written with correct info'):
            # Verify that json.dump() was called with the correct data
            expected_content = {
                'project_id': 'project-1234',
                'folder_name': 'test_folder'
            }

            mock_file_handle = mock_open()
            # handle = mock_of_open
            # mock_of_open.write.assert_called_once()
            expected_json_content = json.dumps(
                expected_content,
                ensure_ascii=False,
                separators=(',', ':')
            )

            # Retrieve all calls to write
            write_calls = [
                call[0] for call in mock_file_handle.write.call_args_list
            ]

            # Join all write calls into a single string
            written_content = ''.join(
                arg for call in write_calls for arg in call
            )

            # Remove whitespace from the JSON content for comparison
            written_content = ''.join(written_content.split())
            expected_json_content = ''.join(expected_json_content.split())

            # Verify the full content matches the expected JSON string
            self.assertEqual(written_content, expected_json_content)
