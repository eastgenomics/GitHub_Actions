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

@patch('check_jobs_complete.dx.DXJob')
class TestGetJobOutputDetails(unittest.TestCase):
    """
    Tests for the DXManage.get_job_output_details() function which
    gets details of the input to an eggd_dias_batch job and the jobs
    and analyses which it launches
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

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    def test_wait_on_done(self, mock_job):
        """
        Test that function held until the eggd_dias_batch job completes
        """
        mock_job_instance = MagicMock()
        mock_job_instance.describe.return_value = {
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
        mock_job.return_value = mock_job_instance

        # call function
        input_dict, launched_jobs = self.dx_manage.get_job_output_details()

        with self.subTest('Test wait on done called once for batch job'):
            mock_job_instance.wait_on_done.assert_called_once()

        with self.subTest('Test print statement'):
            stdout = self.capsys.readouterr().out

            assert (
                'Querying details for eggd_dias_batch job job-123'
            ) in stdout, (
                'Script not querying correct job details'
            )

        with self.subTest('Test input dict returned correctly'):
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

        with self.subTest('Test launched jobs correct'):
            assert launched_jobs == [
                'job-54321', 'job-67890', 'analysis-123'
            ], 'Launched jobs not returned correctly'

@patch('check_jobs_complete.dx.DXAnalysis')
@patch('check_jobs_complete.dx.DXJob')
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

    def test_all_jobs_complete_successfully(
        self, mock_job, mock_analysis
    ):
        """
        Test wait_done() waits for all jobs and analyses to complete
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        # mock_job.return_value.wait_on_done = MagicMock()
        # mock_analysis.return_value.wait_on_done = MagicMock()

        mock_job_instance = MagicMock()
        mock_analysis_instance = MagicMock()

        mock_job.return_value = mock_job_instance
        mock_analysis.return_value = mock_analysis_instance

        # Run
        self.dx_manage.wait_on_done(launched_job_ids)

        # Assert wait_on_done is called the correct number of times for
        # each job/analysis
        with self.subTest('Test wait on done called once for each job'):
            self.assertEqual(mock_job_instance.wait_on_done.call_count, 2)

        with self.subTest('Test wait on done called once for the analysis'):
            self.assertEqual(
                mock_analysis_instance.wait_on_done.call_count, 1
            )

    def test_correct_error_raised_on_job_failing(
        self, mock_job, mock_analysis
    ):
        """
        Test if launched job fails, this is caught and raised
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        mock_job_instance = MagicMock()
        mock_job.return_value = mock_job_instance
        mock_job_instance.wait_on_done.side_effect = (
            dx.exceptions.DXJobFailureError("Oh no, the job failed")
        )

        with pytest.raises(
            dx.exceptions.DXJobFailureError, match='Oh no, the job failed'
        ):
            self.dx_manage.wait_on_done(launched_job_ids)

    def test_correct_error_on_analysis_failing(self, mock_job, mock_analysis):
        """
        If launched analysis fails, test this is caught and exits
        """
        launched_job_ids = ['job-123', 'analysis-123', 'job-345']

        mock_analysis_instance = MagicMock()
        mock_analysis.return_value = mock_analysis_instance

        mock_analysis_instance.wait_on_done.side_effect = (
            dx.exceptions.DXJobFailureError("Oh no, the analysis failed")
        )

        with pytest.raises(
            dx.exceptions.DXJobFailureError,
            match='Oh no, the analysis failed'
        ):
            self.dx_manage.wait_on_done(launched_job_ids)


if __name__ == '__main__':
    unittest.main()
