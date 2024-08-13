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
        launched_jobs_list : list
            list of jobs/analyses launched
        input_dict : dict
            dictionary of inputs to the test job
        """
        # Wait on eggd_dias_batch job to finish to get info
        dx.DXJob(dxid=self.args.job_id).wait_on_done()

        print("All testing jobs set off successfully")

        print(f"Querying details for eggd_dias_batch job {self.args.job_id}")
        # Describe the job to get the input and output launched jobs
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
            ID of the job run in DNAnexus
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
    def wait_on_done(all_job_ids) -> None:
        """
        Hold this Action until all job(s) launched have completed successfully

        Parameters
        ----------
        all_job_ids : list
            list of jobs/analyses launched by GitHub Actions
        """

        print(
            f'Holding check until {len(all_job_ids)} job(s)/analyses complete'
        )

        # Wait til all things launched by eggd_dias_batch complete successfully
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
    Run main functions for getting or creating a DX test project
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
