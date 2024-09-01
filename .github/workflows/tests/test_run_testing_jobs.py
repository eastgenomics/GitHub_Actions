import dxpy as dx
import json
import os
import pytest
import re
import sys
import unittest

from unittest.mock import patch, mock_open, MagicMock

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from run_testing_jobs import DXManage


class TestReadInConfig(unittest.TestCase):
    """
    Test the DXManage().read_in_config() function which reads a JSON
    file in to a dict
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.config_info = 'test_config.json'
        self.dx_manage = DXManage(self.mock_args)

    def test_error_raised_when_config_not_json(self):
        """
        Test error raised when the config file given to read_in_config is not
        JSON file
        """
        # Set up txt file not JSON as input
        self.mock_args.config_info = 'test_config.txt'

        expected_error = (
            'Error: invalid config file given - not a JSON file'
        )

        # Run & assert
        with pytest.raises(RuntimeError, match=expected_error):
            self.dx_manage.read_in_config()

    read_data = {
        "updated": {
            "dxid": "file-123"
        }
    }
    @patch('run_testing_jobs.json.load')
    @patch(
        'run_testing_jobs.open',
        new_callable=mock_open,
        read_data=json.dumps(read_data)
    )
    def test_read_in_config_successfully(self, mock_file_read, mock_load):
        """
        Test config is read in successfully
        """
        # Mock a valid JSON file with minimal keys
        mock_load.return_value = {
            "updated": {
                "dxid": "file-123"
            }
        }

        # Read in
        config_info = self.dx_manage.read_in_config()

        with self.subTest('Config info read in correctly'):
            assert config_info == ({
                "updated": {
                    "dxid": "file-123"
                }
            }), "Config info not read in correctly"

        with self.subTest('File opened and read in'):
            mock_file_read.assert_called_once_with(
                'test_config.json', 'r', encoding='utf8'
            )


class TestGetCNVCallingJobID(unittest.TestCase):
    """
    Test the DXManage().get_cnv_calling_job_id() function which gets the
    CNV calling job which was launched by the original 002 eggd_dias_batch job
    provided as a job to re-run within the GitHub Actions variables
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.assay_job_id = 'job-123'
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.DXJob')
    @patch('run_testing_jobs.dx.describe')
    def test_get_cnv_calling_job_id_single_job(
        self, mock_describe, mock_dxjob
    ):
        """
        Test the CNV calling job ID is returned successfully and describe
        calls are made correctly
        """
        # Mock job describe
        mock_dxjob_instance = mock_dxjob.return_value
        mock_dxjob_instance.describe.return_value = {
            'output': {
                'launched_jobs': 'job-345,job-456,analysis-789,analysis-012'
            }
        }
        mock_describe.side_effect = (
            lambda job_id: {'name': 'GATKgCNV'}
            if job_id == 'job-456' else {'name': 'OtherJob'}
        )

        # Get the job ID
        cnv_calling_job_id = self.dx_manage.get_cnv_calling_job_id()

        with self.subTest('CNV calling job ID returned correctly'):
            assert cnv_calling_job_id == 'job-456', (
                "CNV calling job ID not returned correctly"
            )

        with self.subTest('Job instance created correctly'):
            mock_dxjob.assert_called_once_with(dxid='job-123')

        with self.subTest('Describe called correct number of times for jobs'):
            mock_describe.assert_any_call('job-345')

        with self.subTest('Describe called correct number of times for jobs'):
            mock_describe.assert_any_call('job-456')

        with self.subTest('Describe called correct number of times for jobs'):
            self.assertEqual(
                mock_describe.call_count, 2
            )

    @patch('run_testing_jobs.dx.DXJob')
    @patch('run_testing_jobs.dx.describe')
    def test_get_cnv_calling_job_id_multiple_cnv_jobs(
        self, mock_describe, mock_dxjob
    ):
        """
        Test that error is raised if multiple CNV jobs were launched by an
        eggd_dias_batch job (shouldn't happen)
        """
        # Setup the job description mock
        mock_dxjob_instance = mock_dxjob.return_value
        mock_dxjob_instance.describe.return_value = {
            'output': {
                'launched_jobs': 'job-345,job-456,analysis-789,analysis-012'
            }
        }

        # Setup the dx.describe mock to return job names with multiple CNV
        # jobs
        mock_describe.side_effect = lambda job_id: {'name': 'GATKgCNV'}

        # Assert that an AssertionError is raised when multiple CNV jobs are
        # found
        expected_error = (
            "Error: No or multiple CNV calling jobs launched by "
            "eggd_dias_batch job job-123 given"
        )
        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_cnv_calling_job_id()


    @patch('run_testing_jobs.dx.DXJob')
    @patch('run_testing_jobs.dx.describe')
    def test_get_cnv_calling_job_id_no_cnv_jobs(
        self, mock_describe, mock_dxjob
    ):
        """
        Test that error is raised if no CNV jobs were launched by an
        eggd_dias_batch job
        """
        # Setup the job description mock
        mock_dxjob_instance = mock_dxjob.return_value
        mock_dxjob_instance.describe.return_value = {
            'output': {
                'launched_jobs': 'job-345,job-456,analysis-789,analysis-012'
            }
        }

        # Setup the dx.describe mock to return job names with multiple CNV jobs
        mock_describe.side_effect = lambda job_id: {'name': 'NotCNV'}

        # Assert that an AssertionError is raised when no CNV jobs are found
        expected_error = (
            "Error: No or multiple CNV calling jobs launched by "
            "eggd_dias_batch job job-123 given"
        )
        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_cnv_calling_job_id()

    @patch('run_testing_jobs.dx.DXJob')
    @patch('run_testing_jobs.dx.describe')
    def test_get_cnv_calling_job_id_no_jobs(
        self, mock_describe, mock_dxjob
    ):
        """
        Test that error is raised if no jobs at all (no CNV calling and
        no eggd_artemis) were launched by an eggd_dias_batch job
        """
        # Setup the job description mock
        mock_dxjob_instance = mock_dxjob.return_value
        mock_dxjob_instance.describe.return_value = {
            'output': {
                'launched_jobs': 'analysis-789,analysis-012'
            }
        }

        # Assert that an AssertionError is raised when no jobs were
        # launched
        expected_error = (
            "Error: No jobs were launched by eggd_dias_batch job "
            "job-123 given"
        )
        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_cnv_calling_job_id()


class TestGetProjectIDFromBatchJob(unittest.TestCase):
    """
    Test DXManage().get_project_id_from_batch_job() function which gets the
    project ID where the original production eggd_dias_batch job was run
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.assay_job_id = 'job-123'
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.describe')
    def test_project_id_returned_correctly(self, mock_describe):
        """
        Test the project ID is returned successfully and describe calls are
        made correctly
        """
        mock_describe.return_value = {
            'state': 'done',
            'project': 'project-5678'
        }

        # Call function
        project_id = self.dx_manage.get_project_id_from_batch_job()

        with self.subTest('Project ID returned correctly'):
            assert project_id == 'project-5678', (
                "Project ID not returned correctly"
            )

    @patch('run_testing_jobs.dx.describe')
    def test_error_raised_if_job_id_invalid(self, mock_describe):
        """
        Test error raised if job ID invalid
        """
        mock_describe.side_effect = dx.exceptions.DXError("Describe failed")

        expected_error = (
                "Cannot call dx describe on job ID job-123 "
                "given as prod job:\n Describe failed"
            )

        with pytest.raises(
            dx.exceptions.DXError, match=expected_error
        ):
            self.dx_manage.get_project_id_from_batch_job()

    @patch('run_testing_jobs.dx.describe')
    def test_error_raised_if_job_not_done(self, mock_describe):
        """
        Test error raised if job given is not in state 'done'
        """
        mock_describe.return_value = {
            'state': 'failed',
            'project': 'project-5678'
        }

        expected_error = re.escape(
            "The production job ID given (job-123) for "
            "the assay as a repository variable did not complete "
            "successfully. Please check this job and instead provide a "
            "job within a 002 project for the relevant assay which did "
            "complete successfully"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_project_id_from_batch_job()


class TestFindAllExecutionsInProject(unittest.TestCase):
    """
    Test DXManage().find_all_executions_in_project() function which finds
    all executions which have been run in a project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.test_project_id = 'project-123'
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.find_executions')
    def test_find_executions_correctly(self, mock_find_executions):
        """
        Test executions are returned correctly when exist
        """
        # Set up the mock find_executions
        mock_find_executions.return_value = [
            {
                'id': 'job-Gq26qv84PZYv69029zV69xVf',
                'describe': {
                    'id': 'job-Gq26qv84PZYv69029zV69xVf',
                    'state': 'done'
                }
            },
            {
                'id': 'job-Gq1ZPKQ4PZYfX032b7q8Kpgk',
                'describe': {
                    'id': 'job-Gq1ZPKQ4PZYfX032b7q8Kpgk',
                    'state': 'terminated'
                }
            }
        ]

        # Call function
        executions = self.dx_manage.find_all_executions_in_project()

        expected_return = [
            {
                'id': 'job-Gq26qv84PZYv69029zV69xVf',
                'describe': {
                    'id': 'job-Gq26qv84PZYv69029zV69xVf',
                    'state': 'done'
                }
            },
            {
                'id': 'job-Gq1ZPKQ4PZYfX032b7q8Kpgk',
                'describe': {
                    'id': 'job-Gq1ZPKQ4PZYfX032b7q8Kpgk',
                    'state': 'terminated'
                }
            }
        ]

        with self.subTest('Executions found correctly'):
            assert executions == expected_return, (
                "Executions not found correctly"
            )

        with self.subTest('Find executions called correctly'):
            mock_find_executions.assert_called_once_with(
                project='project-123',
                describe={
                    'fields': {
                        'state': True
                    }
                }
            )

    @patch('run_testing_jobs.dx.find_executions')
    def test_find_all_executions_empty(self, mock_find_executions):
        """
        Test empty list returned if no executions run in project
        """
        # Setup: Mocking find_executions to return an empty list
        mock_find_executions.return_value = []

        # Call the method
        result = self.dx_manage.find_all_executions_in_project()

        # Assert the function returns an empty list
        self.assertEqual(result, [])


class TestFindNonTerminalJobs(unittest.TestCase):
    """
    Test DXManage().find_non_terminal_jobs() function which finds all
    jobs which are not completed/failed/terminated
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    def test_find_non_terminal_jobs_when_all_terminal(self):
        """
        Test no jobs are returned when all are in non-running state
        """
        # Set up the mock find_executions
        mock_executions = [
            {
                'id': 'job-123',
                'describe': {
                    'id': 'job-123',
                    'state': 'done'
                }
            },
            {
                'id': 'job-345',
                'describe': {
                    'id': 'job-345',
                    'state': 'terminated'
                }
            },
            {
                'id': 'job-678',
                'describe': {
                    'id': 'job-678',
                    'state': 'failed'
                }
            }
        ]

        # Call function
        non_terminal_jobs = self.dx_manage.find_non_terminal_jobs(
            mock_executions
        )

        assert non_terminal_jobs == [], (
            "Non-terminal jobs found when all jobs are terminal"
        )

    @patch('run_testing_jobs.dx.describe')
    def test_find_non_terminal_jobs_when_one_running(self, mock_describe):
        """
        Test jobs returned when in non-terminal state
        """
        # Set up the mock return from find_executions
        mock_executions = [
            {
                'id': 'job-123',
                'describe': {
                    'id': 'job-123',
                    'state': 'done'
                }
            },
            {
                'id': 'job-345',
                'describe': {
                    'id': 'job-345',
                    'state': 'running'
                }
            },
            {
                'id': 'job-789',
                'describe': {
                    'id': 'job-345',
                    'state': 'waiting'
                }
            }
        ]

        mock_describe.side_effect = (
            lambda job_id: {'describe': {'name': f'Job {job_id}'}}
        )

        # Call function
        non_terminal_jobs = self.dx_manage.find_non_terminal_jobs(
            mock_executions
        )

        with self.subTest('Non-terminal jobs returned correctly'):
            assert non_terminal_jobs == ['job-345','job-789'], (
                "Non-terminal jobs not returned correctly"
            )

        with self.subTest('Print statements as expected'):
            stdout = self.capsys.readouterr().out

            expected_print = (
                "Running jobs will be terminated:\n"
                "job-345 - Job job-345\n"
                "job-789 - Job job-789\n"
            )

            assert expected_print in stdout, (
                "Info on jobs to be terminated not printed as expected"
            )


class TestFindMultiqcReportInProj(unittest.TestCase):
    """
    Tests DXManage().find_multiqc_report_in_proj() function which finds the
    MultiQC report file in a project
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.find_data_objects')
    def test_multiqc_report_id_returned_correctly(self, mock_find):
        """
        Test MultiQC report file ID is returned correctly
        """
        # mock minimal DX find response
        mock_find.return_value = [
            {
                'project': 'project-1234',
                'name': 'run_report_multiqc.html',
                'id': 'file-1234'
            }
        ]

        multiqc_id = self.dx_manage.find_multiqc_report_in_proj(
            'project-1234'
        )

        assert multiqc_id == 'project-1234:file-1234', (
            "MultiQC report ID not returned correctly"
        )

    @patch('run_testing_jobs.dx.find_data_objects')
    def test_error_raised_when_multiple_multiqc_reports_found(
        self, mock_find
    ):
        """
        Test error is raised if there are multiple MultiQC files found
        in a project
        """
        mock_find.return_value = [
            {
                'project': 'project-1234',
                'name': 'report1_multiqc.html',
                'id': 'file-1234'
            },
            {
                'project': 'project-1234',
                'name': 'report2_multiqc.html',
                'id': 'file-5678'
            }
        ]

        expected_error = (
            "Error: No or multiple MultiQC report\\(s\\) found in project"
            " project-1234"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.find_multiqc_report_in_proj('project-1234')

    @patch('run_testing_jobs.dx.find_data_objects')
    def test_error_if_no_multiqc_found(self, mock_find):
        """
        Test error raised if no MultiQC report file found in a project
        """
        mock_find.return_value = []

        expected_error = (
            "Error: No or multiple MultiQC report\\(s\\) found in project"
            " project-1234"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.find_multiqc_report_in_proj('project-1234')
