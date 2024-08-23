"""
Script to check the testing analyses/jobs complete successfully
"""

import argparse
import dxpy as dx


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments to check jobs complete

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description='Information required to check jobs complete successfully'
    )

    parser.add_argument(
        '-i',
        '--job_id',
        required=True,
        type=str,
        help="DX ID of the job which was set off in previous step(s)"
    )

    parser.add_argument(
        '-o',
        '--outfile_name',
        required=True,
        type=str,
        help="Name of the file to write the original job command to"
    )

    return parser.parse_args()


class DXManage():
    """
    Methods for generic handling of dx related things
    """
    def __init__(self, args) -> None:
        self.args = args

    def get_job_output_details(self):
        """
        Get describe details for all output files from a job

        Returns
        -------
        input_dict : dict
            dictionary of inputs to the test job
        Example:
        {
            'assay': 'TWE',
            'artemis': True,
            'single_output_dir': (
                '/GitHub_Actions_run-10454164028/output/TWE-240802_1153'
            ),
            'snv_reports': True,
            'qc_file': {
                '$dnanexus_link': 'file-GpfPFJ84fj3x93YZP4V4y8K1'
            },
            'manifest_files': [{
                '$dnanexus_link': 'file-GpfQ1PQ4fj3QzGV8p1y3jY3f'
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
                '$dnanexus_link': 'file-Gq1ZGjQ4PZYy7pJGGXyJ763x'
            },
            'multiqc_report': {
                '$dnanexus_link': 'file-GpfK94Q49ppgpjF7gKX5Fkvb'
            },
            'sample_limit': 1
        }
        launched_jobs_list : list
            list of DX IDs for jobs/analyses launched
        """
        # Wait on eggd_dias_batch job to finish so we can get launched jobs
        # info
        dx.DXJob(dxid=self.args.job_id).wait_on_done()

        print("All testing jobs set off successfully")

        print(f"Querying details for eggd_dias_batch job {self.args.job_id}")
        # Describe the job to get the input and output launched jobs
        # we only want to do this when the job is finished so we can get the
        # outputs - these don't exist earlier
        job_details = dx.DXJob(dxid=self.args.job_id).describe()
        input_dict = job_details.get('input')

        # Get output launched jobs
        job_output_ids = job_details.get('output').get('launched_jobs')
        launched_jobs_list = job_output_ids.split(',')

        return input_dict, launched_jobs_list

    @staticmethod
    def write_out_original_job_command(job_inputs, out_name):
        """
        Reconstruct the original job command and write to file

        Parameters
        ----------
        job_inputs : dict
            Inputs of the job which has been run in DNAnexus
        out_name : str
            name of the file to write the original job command to
        """
        # Format the inputs nicely with '-i', equals, and \ for easy reading
        dict_list = [f'-i{k}={v} \\' for k, v in job_inputs.items()]

        print("dx run eggd_dias_batch\n")
        print("\n".join(dict_list))

        # Write out to txt file
        with open(out_name, 'w', encoding='utf8') as out_file:
            print("dx run eggd_dias_batch", file=out_file)
            print("\n".join(dict_list), file=out_file)

    @staticmethod
    def wait_on_done(all_job_ids):
        """
        Hold the GitHub Actions run until all job(s) launched have completed
        successfully

        Parameters
        ----------
        all_job_ids : list
            list of jobs/analyses launched by GitHub Actions

        Raises
        ------
        DXJobFailureError
            If any of the jobs/analyses launched by our job fail or are
            terminated
        """
        print(
            f'Holding check until {len(all_job_ids)} job(s)/analyses complete'
        )

        # Wait for all things launched by eggd_dias_batch (e.g. reports and
        # artemis) to complete successfully
        for job in all_job_ids:
            # check if it's a job - usually the artemis job
            if job.startswith('job-'):
                try:
                    dx.DXJob(dxid=job).wait_on_done()
                except dx.exceptions.DXJobFailureError as err:
                    # dx job error raised (i.e. failure, timed out, terminated)
                    raise dx.exceptions.DXJobFailureError(
                        f"Job {job} failed:\n\n{err}"
                    )
            else:
                # it's an analysis - CNV calling/reports
                try:
                    dx.DXAnalysis(dxid=job).wait_on_done()
                except dx.exceptions.DXJobFailureError as err:
                    # dx analysis error raised
                    print(
                        "Stages of analysis failed:"
                    )
                    raise dx.exceptions.DXJobFailureError(
                        f"Analysis with ID {job} failed:\n\n{err}"
                    )

        print("All jobs completed successfully!")


def main():
    """
    Run main functions for checking that the jobs we've run complete
    successfully
    """
    args = parse_args()
    dx_manage = DXManage(args)
    job_inputs, launched_jobs = dx_manage.get_job_output_details()
    dx_manage.write_out_original_job_command(
        job_inputs,
        args.outfile_name
    )
    dx_manage.wait_on_done(launched_jobs)


if __name__ == '__main__':
    main()
