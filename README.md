# GitHub_Actions
Repository to hold GitHub Actions automated workflows.

## Test changes workflow
### Introduction
This workflow automates some integration testing of Dias batch configuration files, which are used as input to the [eggd_dias_batch](https://github.com/eastgenomics/eggd_dias_batch) app.

### Pre-requisites
Certain secrets and variables must be set in each repository which holds the configuration file(s) ([egg4_dias_TWE_config](https://github.com/eastgenomics/egg4_dias_TWE_config) and/or [egg5_dias_CEN_config](https://github.com/eastgenomics/egg5_dias_CEN_config)), as these secrets/variables are used within the workflow. These can be set in each repository under `Settings/Secrets and variables/Actions/`.

#### Secrets

| Secret  | Type | Example | Description |
| --- | --- | --- | --- |
| DX_TOKEN  | `str` | `a1b2c3`  | Authorisation token for DNAnexus. Requires permissions to view, create projects, upload and run jobs.

#### Variables

| Variable  | Type | Example | Description  |
| --- | --- | --- | --- |
| CONFIG_PATH  | `str` | `project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/dias_batch_configs` | The path in 001_Reference where production eggd_dias_batch configuration files are stored. |
| PROD_JOBS  | `str` | `'{"TWE": "job-GpfQ2fQ4fj3XG67xkbPFXKZg","CEN": "job-Gpbq1q847X23zF1568K3x3q8"}'` | DNAnexus IDs for eggd_dias_batch jobs which were run in production 002 projects for each assay which will be re-run with the updated configuration file for testing. |
| TEST_SAMPLE_LIMIT  | `int` | 2 | The number of samples to set off reports jobs for. |
| RUN_CNV_CALLING  | `bool` | True | If True and the configuration file being updated is CEN, re-runs CNV calling and if False does not. If it is a CEN config being updated and RUN_CNV_CALLING is set to False, will re-use outputs from the CNV calling job which was launched by the original eggd_dias_batch job provided in `PROD_JOBS`. |
| DEVELOPMENT | `bool` | False | If True and a 004 DNAnexus project is created, adds `'_GitHub_Actions_'` to its name. |
| WORKFLOW_BRANCH | `str` | `main` | The name of the branch of the GitHub_Actions repository to check out. |

### Description
#### Diagram

#### Workflow trigger
The workflow will run automatically when a pull request (PR) is opened (to any branch) and a JSON file has been updated in this PR. It will also re-run when a new commit is pushed to the HEAD ref of a PR while it is open (and a JSON file was updated in the push).

###### Trigger caveats
If multiple pushes are made while the workflow is running, the previous workflow run will be cancelled so only one instance of the workflow is running at a time. The workflow will also terminate any jobs currently running in the DNAnexus 004 project created/found to prevent jobs being left running with an outdated configuration file.

#### How it works
The workflow is held within the `test_changes.yml` file and consists of two jobs:
- `get_changed_json_files`
  - This finds how many JSON files have been updated within the PR.
    - If 1 JSON file has been updated, the `upload_and_test` workflow will be run.
    - If this is > 1, the upload_and_test job will not be run and the workflow will end.
- `upload_and_test`
  - Set up Python
  - Install dependencies
  - Get or create 004 test project
    - If DEVELOPMENT, the 

  - Finds or creates a 004 DNAnexus project which has the name of the updated configuration file in its project name to be used for testing.
    - A folder is also created in this project, named with the GitHub Actions run ID and a timestamp the workflow is run.
  - Uploads the updated configuration file to DNAnexus within the folder created in the above step.
  - Based on a production eggd_dias_batch job for the relevant assay, copies over the whole Dias single folder (and the QC status file) from the original 002 project. This is because eggd_artemis requires files to exist in the same project where it is run.
    - If these files already exist in the 004 project, these are moved to the folder for the current GitHub Actions workflow run.
  - Creates a diff report of this configuration file compared to the highest version for the relevant assay in the DNAnexus 001_Reference project and uploads the diff report as an artifact to the GitHub Actions run.
  - Using a production eggd_dias_batch job for the relevant assay, re-runs the job in the 004 project, replacing the configuration file with the one which has been updated in the PR (and re-running CNV calling if this is set to True as a repository variable).
  - Holds the workflow to check that all jobs launched in DNAnexus complete successfully; if so, the Actions workflow check completes successfully.


### Running
#### Calling the workflow from another repository
The workflow can be called from a separate GitHub repository by creating a YML file within the separate repository within a `.github/workflows` directory which references the re-usable workflow in this repository. This requires setting the GitHub Actions secrets and variables in the caller repository and passing them to the re-usable workflow. Example YML file:
```
name: Test dias batch configs

on:
  pull_request:
    types:
      - opened
      - synchronize
    paths:
      - '**/*.json'

jobs:
  test_config:
    uses: eastgenomics/GitHub_Actions/.github/workflows/test_changes.yml@main
    with:
      config_path: ${{ vars.CONFIG_PATH }}
      prod_jobs: ${{ vars.PROD_JOBS }}
      test_sample_limit: ${{ vars.TEST_SAMPLE_LIMIT }}
      run_cnv_calling: ${{ vars.RUN_CNV_CALLING }}
      development: ${{ vars.DEVELOPMENT }}
      workflow_branch: ${{ vars.WORKFLOW_BRANCH }}
    secrets:
      DX_TOKEN: ${{ secrets.DX_TOKEN }}
```
The workflow will then run when a PR is opened in the caller repository and a JSON file is changed.