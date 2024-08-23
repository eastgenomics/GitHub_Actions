import json
import os
import pytest
import sys
import unittest
from unittest import mock
from unittest.mock import patch, mock_open, MagicMock

sys.path.append(os.path.abspath(
    os.path.join(os.path.realpath(__file__), '../../')
))

from get_production_config import DXManage, parse_args


class TestReadInConfig(unittest.TestCase):
    """
    Tests reading in the config file DXManage.read_in_config()
    """
    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.config_path = 'project-123:/path/to/configs'
        self.mock_args.input_config = 'test_config.json'
        self.mock_args.file_id = 'file-123'
        self.dx_manage = DXManage(self.mock_args)

    def test_error_raised_when_config_not_json(self):
        """
        Test error raised when the config file given to read_in_config is not
        JSON file
        """
        # Set up txt file not JSON as input
        self.mock_args.input_config = 'test_config.txt'

        expected_error = (
            'Error: invalid config file given - not a JSON file'
        )

        # Run & assert
        with pytest.raises(RuntimeError, match=expected_error):
            self.dx_manage.read_in_config()

    @patch('get_production_config.json.load')
    @patch(
        'get_production_config.open', new_callable=mock_open,
        read_data='{"assay": "TWE"}'
    )
    def test_read_in_config_successfully(self, mock_file_read, mock_load):
        """
        Test config is read in successfully
        """
        # Mock a valid JSON file with the required 'assay' field
        mock_load.return_value = {
            "assay": "TWE"
        }

        # Read in
        config_contents, assay = self.dx_manage.read_in_config()

        assert assay == 'TWE', 'Assay not returned correctly'
        assert config_contents['assay'] == 'TWE', (
            'Assay not added correctly'
        )
        assert config_contents['name'] == 'test_config.json', (
            "Config file name not added correctly"
        )
        assert config_contents['dxid'] == 'file-123', (
            "Config DX file ID not added correctly"
        )

    @patch('get_production_config.json.load')
    @patch(
        'get_production_config.open', new_callable=mock_open,
        read_data='{}'
    )
    def test_error_raised_if_no_assay_field(self, mock_file_read, mock_load):
        """
        Test that error raised if the config file which has been
        updated has no assay field
        """
        mock_load.return_value = {}

        expected_error = (
            "The updated config test_config.json does not have "
            "assay field"
        )
        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.read_in_config()


class TestGetJsonConfigsInDNAnexus(unittest.TestCase):
    """
    Tests for the DXManage().get_json_configs_in_DNAnexus() function which
    searches a path in DNAnexus for JSON files
    """

    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.config_path = 'project-123:/path/to/configs'
        self.mock_args.input_config = 'test_config.json'
        self.mock_args.file_id = 'file-123'
        self.dx_manage = DXManage(self.mock_args)

    @patch('get_production_config.re.match')
    def test_get_json_configs_in_DNAnexus_invalid_path(self, mock_re_match):
        """
        Test error is raised when invalid 001_Reference path to
        eggd_dias_batch configs is given
        """
        # Arrange
        mock_re_match.return_value = False
        self.mock_args.config_path = 'invalid-path-format'

        expected_error = (
            "Path to assay configs appears invalid: invalid-path-format"
        )

        # Act & Assert
        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_json_configs_in_DNAnexus()

    @patch('get_production_config.dx.find_data_objects')
    def test_configs_found_successfully(self, mock_find):
        """
        Test functionality when configs found successfully
        """
        # Arrange
        mock_find.return_value = [
            {
                'project': 'project-123',
                'id': 'file-678',
                'describe': {
                    'name': 'test_config.json',
                    'archivalState': 'live',
                    'folder': '/path/to/configs'
                }
            }
        ]

        # Act
        config_files = self.dx_manage.get_json_configs_in_DNAnexus()

        # Assert
        assert config_files == mock_find.return_value, (
            'Config files not returned as expected'
        )

    @patch('get_production_config.dx.find_data_objects')
    @patch('get_production_config.re.match')
    def test_error_raised_when_no_config_files_found(
        self, mock_re_match, mock_find
    ):
        """
        AssertionError should be raised if no JSON files found, patch
        the return of the dxpy.find_data_objects() call to be empty and
        check we raise an error correctly
        """
        mock_re_match.return_value = True
        mock_find.return_value = []

        expected_error = (
            "No config files found in given path: "
            "project-123:/path/to/configs"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.get_json_configs_in_DNAnexus()


class TestFilterHighestBatchConfig(unittest.TestCase):
    """
    Tests for DXManage.filter_highest_batch_config() function which filters
    batch configs to get the highest version for an assay
    """

    def setUp(self):
        # Set up a default mock args object
        self.mock_args = MagicMock()
        self.mock_args.config_path = 'project-123:/path/to/configs'
        self.mock_args.input_config = 'test_config.json'
        self.mock_args.file_id = 'file-123'
        self.dx_manage = DXManage(self.mock_args)

        self.loads_patch = mock.patch('get_production_config.json.load')
        self.file_patch = mock.patch('get_production_config.dx.DXFile')
        self.read_patch = mock.patch('get_production_config.dx.DXFile.read')

        # create mocks to reference
        self.mock_loads = self.loads_patch.start()
        self.mock_file = self.file_patch.start()
        self.mock_read = self.read_patch.start()

    def tearDown(self):
        self.mock_loads.stop()
        self.mock_file.stop()
        self.mock_read.stop()

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        """
        Capture stdout to provide it to tests
        """
        self.capsys = capsys

    def test_highest_version_correctly_selected(self):
        """
        Test that the highest version of the config is correctly found
        """
        # set minimal describe output of files found in DNAnexus
        # with describe output
        # need a dict per mock_read return values to test with
        config_files = [
            {
                'project': 'project-123',
                'id': 'file-345',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config1.json'
                },
            },
            {
                'project': 'project-123',
                'id': 'file-456',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config2.json'
                }
            }
        ] * 2

        # self.mock_file.return_value = dx.DXFile

        # patch the output from DXFile.read() to simulate looping over
        # the return of reading multiple configs
        self.mock_file.return_value.read.side_effect = [
            json.dumps({'assay': 'TWE', 'version': '1.0.0'}),
            json.dumps({'assay': 'TWE', 'version': '1.0.10'}),
            json.dumps({'assay': 'TWE', 'version': '1.1.11'}),
            json.dumps({'assay': 'TWE', 'version': '1.2.1'})
        ]

        highest_config = self.dx_manage.filter_highest_batch_config(
            config_files, 'TWE'
        )

        assert highest_config['version'] == '1.2.1', (
            "Incorrect config file version returned"
        )

    def test_error_raised_when_no_config_file_found_for_assay(self):
        """
        Test that error is raised if there are no config files found for
        the assay given in the 001_Reference path
        """
        config_files = [
            {
                'project': 'project-123',
                'id': 'file-345',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config1.json'
                },
            }
        ]

        self.mock_file.return_value.read.return_value = (
            json.dumps({'assay': 'NONE', 'version': '1.0.0'})
        )

        expected_error = (
            "No config file was found for TWE in project-123:/path/to/configs"
        )

        with pytest.raises(AssertionError, match=expected_error):
            self.dx_manage.filter_highest_batch_config(config_files, 'TWE')

    def test_multiple_files_raises_error(self):
        """
        Test that when more than one file found for same config version
        that a RuntimeError is raised since we can't unambiguously tell
        which file to use
        """
        config_files = [
            {
                'project': 'project-123',
                'id': 'file-345',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config1.json'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-456',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config2.json'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-567',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config3.json',
                }
            }
        ]

        # set two files for same assay to have same highest version
        self.mock_file.return_value.read.side_effect = [
            json.dumps({'assay': 'TWE', 'version': '1.0.0'}),
            json.dumps({'assay': 'TWE', 'version': '1.2.1'}),
            json.dumps({'assay': 'TWE', 'version': '1.2.1'})
        ]

        expected_error = (
            "Error: more than one file found for highest version of "
            "TWE configs. Files found:\\n\\tconfig2.json \(file-456\)"
            "\\n\\tconfig3.json \(file-567\)"
        )

        with pytest.raises(RuntimeError, match=expected_error):
            self.dx_manage.filter_highest_batch_config(config_files, 'TWE')

    def test_non_live_config_files_skipped(self):
        """
        Test that any config files which are not live are skipped when
        finding the highest version
        """
        # set up 4 config files found, 1 is live and rest are other states of
        # non-live
        config_files = [
            {
                'project': 'project-123',
                'id': 'file-345',
                'describe': {
                    'archivalState': 'live',
                    'name': 'config1.json'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-456',
                'describe': {
                    'archivalState': 'archived',
                    'name': 'config2.json'
                }
            },
            {
                'project': 'project-123',
                'id': 'file-567',
                'describe': {
                    'archivalState': 'archival',
                    'name': 'config3.json',
                }
            },
            {
                'project': 'project-123',
                'id': 'file-678',
                'describe': {
                    'archivalState': 'unarchiving',
                    'name': 'config4.json',
                }
            }
        ]

        self.mock_file.return_value.read.return_value = (
            json.dumps({'assay': 'TWE', 'version': '1.0.0'})
        )

        highest_config = self.dx_manage.filter_highest_batch_config(
            config_files, 'TWE'
        )

        assert highest_config['version'] == '1.0.0', (
            "Non-live config files not correctly skipped"
        )

        # Check that expected warnings are printed
        stdout = self.capsys.readouterr().out

        expected_warning = (
            "Config file not in live state - will not be used: "
            "config2.json (file-456)\n"
            "Config file not in live state - will not be used: "
            "config3.json (file-567)\n"
            "Config file not in live state - will not be used: "
            "config4.json (file-678)\n"
        )

        assert expected_warning in stdout, (
            "Warnings not printed for non-live config files"
        )


class TestSaveConfigInfoToDict(unittest.TestCase):
    """
    Test that config file info saved to dictionary correctly
    """
    def setUp(self):
        self.mock_args = MagicMock()
        self.dx_manage = DXManage(self.mock_args)

    def test_config_info_saved_correctly(self):
        """
        _summary_
        """
        changed_config = {
            'assay': 'TWE',
            'name': 'changed_config.json',
            'version': "1.3.0",
            'dxid': 'file-123'
        }
        prod_config = {
            'assay': 'TWE',
            'name': 'prod_config.json',
            'version': '1.2.0',
            'dxid': 'file-456'
        }

        expected_output = {
            'assay': 'TWE',
            'updated': {
                'name': 'changed_config.json',
                'assay': 'TWE',
                'version': '1.3.0',
                'dxid': 'file-123'
            },
            'prod': {
                'name': 'prod_config.json',
                'assay': 'TWE',
                'version': '1.2.0',
                'dxid': 'file-456'
            }
        }

        config_matching_info = self.dx_manage.save_config_info_to_dict(
            'TWE', changed_config, prod_config
        )

        assert config_matching_info == expected_output, (
            "Config info not saved to dict correctly"
        )
