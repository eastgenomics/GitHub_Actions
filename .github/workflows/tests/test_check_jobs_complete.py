import argparse
import dxpy as dx
import os
import pytest
import sys
import unittest
from unittest import mock
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from check_jobs_complete import DXManage, parse_args


class TestGetJobOutputDetails(unittest.TestCase):
    """
    Tests for the DXManage.wait_on_done() function which waits until all jobs
    which are launched by the job set off by the workflow (eggd_dias_batch)
    complete successfully
    """
    @classmethod
    def setUpClass(cls):
        sys.argv = [
            'check_jobs_complete.py',
            '-i', 'job-123',
            '-o', 'job_command_info.txt'
        ]
        args = parse_args()

        cls.dx_manage = DXManage(args)

    def setUp(self):
        """
        Set up test class-wide patches
        """
        # set up patches for each function call
        self.job_patch = mock.patch('check_jobs_complete.dx.DXJob')
        self.wait_patch = mock.patch(
            'check_jobs_complete.dx.DXJob.wait_on_done'
        )

        # create our mocks to reference
        self.mock_job = self.job_patch.start()
        # self.mock_describe = self.describe_patch.start()
        self.mock_wait = self.wait_patch.start()

        # define some returns in expected format to use for the mocks
        self.mock_wait.return_value = None

        # Mock the DXJob instance
        self.mock_job_instance = self.mock_job.return_value
        # dx.describe call is on a job ID to get the input details and the
        # launched jobs, patch in response with required keys
        self.mock_job_instance.describe.return_value = {
            'input': {
                'assay': 'TWE',
                'artemis': True,
                'single_output_dir': (
                    '/GitHub_Actions_run-123/output/TWE-date_time'
                ),
                'snv_reports': True,
                'qc_file': {'$dnanexus_link': 'file-123'},
                'manifest_files': [{
                    '$dnanexus_link': 'file-234'
                }],
                'assay_config_dir': (
                    'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                    'dias_batch_configs/'
                ),
                'split_tests': True,
                'exclude_controls': True,
                'unarchive': False,
                'unarchive_only': False,
                'assay_config_file': {
                    '$dnanexus_link': 'file-345'
                },
                'multiqc_report': {
                    '$dnanexus_link': 'file-456'
                },
                'sample_limit': 1
            },
            'output': {
                'launched_jobs': 'job-54321,job-67890,analysis-123'
            }
        }

    def tearDown(self):
        """
        Remove test class-wide patches
        """
        self.mock_job.stop()
        self.mock_wait.stop()

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    def test_get_output(self):
        """
        Test with everything patched that no errors are raised
        """
        self.dx_manage.get_job_output_details()

    def test_wait_on_done(self):
        """
        Test that function held until the eggd_dias_batch job completes
        """
        _, _ = self.dx_manage.get_job_output_details()

        stdout = self.capsys.readouterr().out

        assert 'All testing jobs set off successfully' in stdout, (
            'Script not waiting until jobs complete'
        )

    def test_print_statement(self):
        """
        Test the job ID queried is printed correctly
        """
        _, _ = self.dx_manage.get_job_output_details()

        stdout = self.capsys.readouterr().out

        assert 'Querying details for eggd_dias_batch job job-123' in stdout, (
            'Script not querying correct job details'
        )

    def test_input_dict_correct(self):
        """
        Test the eggd_dias_batch job input dict is returned correctly
        """
        input_dict, _ = (
            self.dx_manage.get_job_output_details()
        )

        assert input_dict == {
            'assay': 'TWE',
            'artemis': True,
            'single_output_dir': (
                '/GitHub_Actions_run-123/output/TWE-date_time'
            ),
            'snv_reports': True,
            'qc_file': {'$dnanexus_link': 'file-123'},
            'manifest_files': [{
                '$dnanexus_link': 'file-234'
            }],
            'assay_config_dir': (
                'project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/'
                'dias_batch_configs/'
            ),
            'split_tests': True,
            'exclude_controls': True,
            'unarchive': False,
            'unarchive_only': False,
            'assay_config_file': {
                '$dnanexus_link': 'file-345'
            },
            'multiqc_report': {
                '$dnanexus_link': 'file-456'
            },
            'sample_limit': 1
        }, 'Job input dict not returned correctly'

    def test_launched_jobs_correct(self):
        """
        Test that the launched jobs are returned in a list
        """
        _, launched_jobs = (
            self.dx_manage.get_job_output_details()
        )

        assert launched_jobs == [
            'job-54321', 'job-67890', 'analysis-123'
        ], 'Launched jobs not returned correctly'


class TestWaitOnDone(unittest.TestCase):
    """
    Tests for the DXManage.wait_on_done() function which waits until all jobs
    and analyses launched by eggd_dias_batch complete successfully
    """
    @classmethod
    def setUpClass(cls):
        sys.argv = [
            'check_jobs_complete.py',
            '-i', 'job-123',
            '-o', 'job_command_info.txt'
        ]
        args = parse_args()

        cls.dx_manage = DXManage(args)

    @patch('check_jobs_complete.dx.DXAnalysis')
    @patch('check_jobs_complete.dx.DXJob')
    def test_all_jobs_complete_successfully(
        self, mock_job, mock_analysis
    ):
        """
        Test wait_done() waits for all jobs and analyses to complete
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        mock_job.return_value.wait_on_done = MagicMock()
        mock_analysis.return_value.wait_on_done = MagicMock()

        # Run
        self.dx_manage.wait_on_done(launched_job_ids)

        # Assert wait_on_done is called the correct number of times for
        # each job/analysis
        self.assertEqual(mock_job.return_value.wait_on_done.call_count, 2)
        self.assertEqual(
            mock_analysis.return_value.wait_on_done.call_count, 1
        )

    @patch('check_jobs_complete.dx.DXJob')
    def test_correct_error_raised_on_job_failing(self, mock_job):
        """
        Test if launched job fails, this is caught and raised
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        mock_job.return_value.wait_on_done.side_effect = (
            dx.exceptions.DXJobFailureError("Oh no, the job failed")
        )

        with pytest.raises(
            dx.exceptions.DXJobFailureError, match='Oh no, the job failed'
        ):
            self.dx_manage.wait_on_done(launched_job_ids)

    @patch('check_jobs_complete.dx.DXAnalysis')
    def test_correct_error_on_analysis_failing(self, mock_analysis):
        """
        If launched analysis fails, test this is caught and exits
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        mock_analysis.return_value.wait_on_done.side_effect = (
            dx.exceptions.DXJobFailureError("Oh no, the analysis failed")
        )

        with pytest.raises(
            dx.exceptions.DXJobFailureError,
            match='Oh no, the analysis failed'
        ):
            self.dx_manage.wait_on_done(launched_job_ids)


if __name__ == '__main__':
    unittest.main()
