# GitHub_Actions
Repository to hold GitHub Actions automated workflows.

## Test changes workflow
### Description
This workflow is held in the `test_changes.yml` file. This workflow runs automatically on each PR (to any branch). The workflow:
- Checks if any configuration files (*.JSON files) are updated within the PR.
- If one configuration file has been updated, the tool:
  - Looks for a 004 project (with any date) which has the name of the updated configuration file in the project name. If this does not exist, one is created, including the date the PR is opened.
  - Uploads the updated configuration file to DNAnexus.
  - Creates a diff report of this configuration file compared to the highest version in 001_Reference (in a directory which is set as a repository variable) and uploads this is an artifact to the GitHub Actions run.
  - Using a production eggd_dias_batch job which is set as a repository variable, re-runs the job using the configuration file which has been updated in the PR.
  - Checks that all jobs launched complete successfully.

While the PR is open, the workflow will be set off again on each push to the branch which is being merged. However, if multiple pushes are made while the workflow is running, the previous workflow run will be cancelled so only one GitHub Actions job is running at a time.

### Required repository secrets and variables
Certain constants must be set in the repository which holds the configuration files, as these are used within the workflow. These must be set within the repository which holds the configuration file(s) in `Settings/Secrets and variables/Actions/`.

#### Secrets

| Secret  | Type | Example | Description |
| --- | --- | --- | --- |
| DX_TOKEN  | `str` | `a1b2c3`  | Authorisation token for DNAnexus. Requires permissions to view, create projects, upload and run jobs.

#### Variables

| Variable  | Type | Example | Description  |
| --- | --- | --- | --- |
| CONFIG_PATH  | `str` | `project-Fkb6Gkj433GVVvj73J7x8KbV:/dynamic_files/dias_batch_configs` | The path in 001_Reference where production configuration files are stored |
| PROD_JOBS  | `str` | `'{"TWE": "job-GpfQ2fQ4fj3XG67xkbPFXKZg","CEN": "job-Gpbq1q847X23zF1568K3x3q8"}'` | Jobs which were run in production 002 projects for each assay which can be used for testing to re-run with the updated configuration file |
| TEST_SAMPLE_LIMIT  | `int` | 2 | The number of samples to set off testing jobs for |
| RUN_CNV_CALLING  | `bool` | True | If True, runs CNV calling and if False does not. If it is a CEN config being updated and RUN_CNV_CALLING is set to False, will re-use inputs from the job given in PROD_JOBS. |