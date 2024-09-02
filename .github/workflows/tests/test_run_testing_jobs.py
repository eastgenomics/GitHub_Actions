import dxpy as dx
import json
import pytest
import re
import unittest

from unittest import mock
from unittest.mock import patch, mock_open, MagicMock

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


class TestCheckDiasSingleVersion(unittest.TestCase):
    """
    Tests for DXManage().check_dias_single_version() function which checks
    the version of Dias single which was used to generate data in the
    002 project which relates to the production eggd_dias_batch job given
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.repo_version = 'v2.5.0'
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    @patch('run_testing_jobs.dx.find_analyses')
    def test_dias_single_version_returned_correctly(self, mock_find):
        """
        Test Dias single version is returned correctly
        """
        # Mock find response
        mock_find.return_value = [
            {
                'id': 'analysis-123',
                'describe': {
                    'id': 'analysis-123',
                    'executableName': 'dias_single_v2.5.0'
                }
            }
        ]

        dias_single_version = (
            self.dx_manage.check_dias_single_version_which_generated_data(
                'project-123'
            )
        )

        assert dias_single_version == 'v2.5.0', (
            "Dias single version not returned correctly"
        )

    @patch('run_testing_jobs.dx.find_analyses')
    def test_error_raised_when_multiple_dias_single_versions_found(
        self, mock_find
    ):
        """
        Test error is raised if multiple Dias single versions found
        """
        mock_find.return_value = [
            {
                'id': 'analysis-123',
                'describe': {
                    'id': 'analysis-123',
                    'executableName': 'dias_single_v2.5.0'
                }
            },
            {
                'id': 'analysis-456',
                'describe': {
                    'id': 'analysis-456',
                    'executableName': 'dias_single_v2.6.0'
                }
            }
        ]

        expected_error = (
            "Error: 2 versions of the dias "
            "single workflow were used to generate data in this project:"
            "\nv2.5.0\n\tv2.6.0\n. This is not expected - please change the"
            " job for this assay in the repository's GA variables"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.check_dias_single_version_which_generated_data(
                'project-123'
            )

    @patch('run_testing_jobs.dx.find_analyses')
    def test_error_raised_when_no_dias_single_version_found(self, mock_find):
        """
        Test error is raised if no Dias single version found
        """
        mock_find.return_value = []

        expected_error = (
            "Error: No Dias single job was found in the project project-123"
            " given"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.check_dias_single_version_which_generated_data(
                'project-123'
            )

    @patch('run_testing_jobs.dx.find_analyses')
    def test_error_raised_when_version_does_not_match_github_repo(
        self, mock_find
    ):
        """
        Test error is raised if no Dias single version found
        """
        self.dx_manage.args.repo_version = 'v2.6.0'

        mock_find.return_value = [
            {
                'id': 'analysis-123',
                'describe': {
                    'id': 'analysis-123',
                    'executableName': 'dias_single_v2.5.0'
                }
            }
        ]

        expected_warning = (
            "Warning: The version of Dias single used to generate data in "
            "this project (v2.5.0) does not match the current "
            "release version for the Dias single GitHub repo "
            "(v2.6.0). \nPlease check that this is "
            "expected and if not, update the job ID given for this assay"
            " within the repository's GitHub Actions variables"
        )

        dias_single_version = (
            self.dx_manage.check_dias_single_version_which_generated_data(
                'project-123'
            )
        )

        stdout = self.capsys.readouterr().out

        assert expected_warning in stdout, (
            "Warning about Dias single version not printed as expected"
        )


class TestUpdateInputsToBatchJob(unittest.TestCase):
    """
    Test DXManage().update_inputs_to_batch_job() function which takes the
    original input from an eggd_dias_batch job and updates them according
    to the eggd_dias_batch config we are updating and what we want to re-run
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.folder_name = '/GitHub_Actions_run-123_240822_1006'
        self.mock_args.test_sample_limit = 1
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.describe')
    def test_inputs_updated_correctly_if_cnv_job_id(self, mock_describe):
        """
        Test inputs are updated correctly if a CNV job ID is given as an input
        """
        # Mock describe response with inputs from CEN eggd_dias_batch job
        mock_describe.return_value = {
            'executableName': 'eggd_dias_batch',
            'input': {
                'assay': 'CEN',
                'single_output_dir': '/output/CEN-240801_1018',
                'cnv_call': True,
                'snv_reports': True,
                'cnv_reports': True,
                'artemis': True,
                'qc_file': {
                    '$dnanexus_link': 'file-XXX'
                },
                'manifest_files': [
                    {'$dnanexus_link': 'file-ABC'}
                ],
                'assay_config_dir': (
                    'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                    'dias_batch_configs/'
                ),
                'split_tests': True,
                'exclude_controls': True,
                'exclude_samples': '123456-123R0123',
                'unarchive': False,
                'unarchive_only': False
            }
        }

        updated_config_id = 'file-892'
        multiqc_report = 'project-123:file-789'
        cnv_job_id = 'job-123'

        # Call function
        app_name, updated_inputs = self.dx_manage.update_inputs_to_batch_job(
            updated_config_id, multiqc_report, cnv_job_id
        )

        with self.subTest('App name returned correctly'):
            assert app_name == 'eggd_dias_batch', (
                "App name not returned correctly"
            )

        with self.subTest('Inputs updated correctly'):
            expected_new_inputs = {
                'assay': 'CEN',
                'single_output_dir': (
                    '/GitHub_Actions_run-123_240822_1006/output/'
                    'CEN-240801_1018'
                ),
                'cnv_call': False,
                'cnv_call_job_id': 'job-123',
                'snv_reports': True,
                'cnv_reports': True,
                'artemis': True,
                'assay_config_file': {
                    '$dnanexus_link': 'file-892'
                },
                'qc_file': {
                    '$dnanexus_link': 'file-XXX'
                },
                'multiqc_report': {
                    '$dnanexus_link': 'file-789'
                },
                'manifest_files': [
                    {'$dnanexus_link': 'file-ABC'}
                ],
                'assay_config_dir': (
                    'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                    'dias_batch_configs/'
                ),
                'split_tests': True,
                'exclude_controls': True,
                'exclude_samples': '123456-123R0123',
                'unarchive': False,
                'unarchive_only': False,
                'sample_limit': 1
            }

            assert updated_inputs == expected_new_inputs, (
                "eggd_dias_batch inputs not updated correctly"
            )

    @patch('run_testing_jobs.dx.describe')
    def test_inputs_updated_correctly_if_no_cnv_job_id(self, mock_describe):
        """
        Test inputs updated correctly if no CNV job ID is given as an input
        """
                # Mock describe response with mimimal inputs
        mock_describe.return_value = {
            'executableName': 'eggd_dias_batch',
            'input': {
                'assay': 'TWE',
                'artemis': True,
                'single_output_dir': '/output/TWE-240426_1508',
                'snv_reports': True,
                'qc_file': {
                    '$dnanexus_link': 'file-XYZ'
                },
                'manifest_files': [
                    {'$dnanexus_link': 'file-ABC'}
                ],
                'assay_config_file': {
                    '$dnanexus_link': 'file-123'
                },
                'assay_config_dir': (
                    'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                    'dias_batch_configs/'
                ),
                'split_tests': True,
                'exclude_controls': True,
                'unarchive': False,
                'unarchive_only': False
            }
        }

        updated_config_id = 'file-892'
        multiqc_report = 'project-123:file-789'
        cnv_job_id = None

        # Call function
        app_name, updated_inputs = self.dx_manage.update_inputs_to_batch_job(
            updated_config_id, multiqc_report, cnv_job_id
        )

        with self.subTest('App name returned correctly'):
            assert app_name == 'eggd_dias_batch', (
                "App name not returned correctly"
            )

        with self.subTest('Inputs updated correctly'):
            expected_new_inputs = {
                'assay': 'TWE',
                'artemis': True,
                'snv_reports': True,
                'single_output_dir': (
                    '/GitHub_Actions_run-123_240822_1006/output/'
                    'TWE-240426_1508'
                ),
                'qc_file': {
                    '$dnanexus_link': 'file-XYZ'
                },
                'manifest_files': [
                    {'$dnanexus_link': 'file-ABC'}
                ],
                'assay_config_file': {
                    '$dnanexus_link': 'file-892'
                },
                'multiqc_report': {
                    '$dnanexus_link': 'file-789'
                },
                'sample_limit': 1,
                'assay_config_dir': (
                    'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                    'dias_batch_configs/'
                ),
                'split_tests': True,
                'exclude_controls': True,
                'unarchive': False,
                'unarchive_only': False
            }

            assert updated_inputs == expected_new_inputs, (
                "eggd_dias_batch inputs not updated correctly when no CNV "
                "calling job ID given so CNV calling should be re-run"
            )


class TestTerminate(unittest.TestCase):
    """
    Tests for DXManage().terminate() function which takes a list of job or
    analysis IDs and calls terminate on them in parallel
    """
    def setUp(self):
        """
        Set up mocks of terminate
        """
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """Capture stdout to provide it to tests"""
        self.capsys = capsys

    @patch('run_testing_jobs.dx.DXJob')
    def test_jobs_terminate(self, mock_dxjob):
        """
        Test when jobs provided they get terminate() called
        """
        # patch job object on which terminate() will get called
        mock_dxjob_instance = MagicMock()
        mock_dxjob.return_value = mock_dxjob_instance

        self.dx_manage.terminate(['job-xxx', 'job-yyy'])

        mock_dxjob.assert_any_call(dxid='job-xxx')
        mock_dxjob.assert_any_call(dxid='job-yyy')
        self.assertEqual(mock_dxjob_instance.terminate.call_count, 2)

    @patch('run_testing_jobs.dx.DXAnalysis')
    def test_analyses_terminate(self, mock_dxanalysis):
        """
        Test when analyses provided they get terminate() called
        """
        # patch analysis object on which terminate() will get called
        mock_dxanalysis_instance = MagicMock()
        mock_dxanalysis.return_value = mock_dxanalysis_instance

        self.dx_manage.terminate(['analysis-xxx', 'analysis-yyy'])

        mock_dxanalysis.assert_any_call(dxid='analysis-xxx')
        mock_dxanalysis.assert_any_call(dxid='analysis-yyy')
        self.assertEqual(mock_dxanalysis_instance.terminate.call_count, 2)

    @patch('run_testing_jobs.dx.DXJob')
    def test_terminate_with_exception(self, mock_dxjob):
        """
        Test exception raised if terminate call fails
        """
        mock_dxjob_instance = MagicMock()
        mock_dxjob.return_value = mock_dxjob_instance

        # Add exceptions as side effects
        mock_dxjob_instance.terminate.side_effect = (
            Exception('termination failed')
        )

        # Run
        self.dx_manage.terminate(['job-123'])

        mock_dxjob.assert_called_once_with(dxid='job-123')

        stdout = self.capsys.readouterr().out

        assert 'Error terminating job job-123: termination failed' in stdout, (
            'Error in terminating job not correctly caught'
        )

class TestSetOffTestJobs(unittest.TestCase):
    """
    Tests for DXManage().set_off_test_jobs() function which sets off a
    DNAnexus eggd_dias_batch job with our inputs
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.folder_name = '/GitHub_Actions_run-123_240822_1006'
        self.mock_args.test_project_id = 'project-234'
        self.mock_args.run_id = 1234
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.dx.DXJob')
    @patch('run_testing_jobs.dx.DXApp')
    def test_set_off_job_correctly(self, mock_app, mock_job):
        """
        Test that eggd_dias_batch job is set off with correct inputs
        """
        # Mock the job and app
        mock_job_instance = MagicMock()
        mock_app_instance = MagicMock()
        mock_app_instance.run.return_value = mock_job_instance

        mock_app.return_value = mock_app_instance
        mock_job_instance.describe.return_value = {'id': 'job-456'}
        mock_job.return_value = mock_job_instance

        app_name = 'eggd_dias_batch'
        job_inputs = {
            'assay': 'TWE',
            'artemis': True,
            'single_output_dir': (
                '/GitHub_Actions_run-123_240822_1006/output/'
                'TWE-240426_1508'
            ),
            'snv_reports': True,
            'qc_file': {
                '$dnanexus_link': 'file-XXX'
            },
            'manifest_files': [
                {'$dnanexus_link': 'file-ABC'}
            ],
            'assay_config_file': {
                '$dnanexus_link': 'file-892'
            },
            'split_tests': True,
            'exclude_controls': True,
            'unarchive': False,
            'unarchive_only': False,
            'multiqc_report': {
                '$dnanexus_link': 'file-789'
            },
            'sample_limit': 1
        }

        # Call the function
        self.dx_manage.set_off_test_jobs(
            job_inputs, app_name
        )

        with self.subTest('Mock app instance created correctly'):
            mock_app.assert_called_once_with(name='eggd_dias_batch')

        with self.subTest('Mock app run with correct inputs'):
            mock_app.return_value.run.assert_called_once_with(
                app_input=job_inputs,
                project='project-234',
                folder='/GitHub_Actions_run-123_240822_1006'
            )

        with self.subTest('Job describe called correctly'):
            mock_job.assert_called_once_with(
                dxid='job-456'
            )

        with self.subTest('Job tagging called correctly'):
            mock_job.return_value.add_tags.assert_called_once_with(
            tags=['GitHub Actions run ID: 1234']
        )


class TestWriteOutJobId(unittest.TestCase):
    """
    Tests for DXManage().write_out_job_id() function which writes out
    the job which has been launched to a file
    """
    def setUp(self):
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    @patch('run_testing_jobs.open', new_callable=mock_open)
    def test_writing_out_job_id(self, mock_open_function):
        """
        Test job ID is written out correctly
        """
        job_id = 'job-1234'
        outfile_name = 'job_id.txt'

        self.dx_manage.write_out_job_id(job_id, outfile_name)

        with self.subTest('Assert file opened correctly'):
            mock_open_function.assert_called_once_with(
                'job_id.txt', 'w', encoding='utf8'
            )

        with self.subTest('Assert job ID written out correctly'):
            file_handle = mock_open_function()
            file_handle.write.assert_called_once_with('job-1234')
