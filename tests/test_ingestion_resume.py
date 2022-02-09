# TODO
#  Resume when another python method fails
#  Resume when tasks parameter is passed
#  Resume when tasks parameter is incompatible with last completed step
#  Annotation only interaction with resume
import os
from itertools import cycle
from subprocess import CalledProcessError
from unittest.mock import patch

from eva_submission.step_management import SubmissionStep
from tests.test_eload_ingestion import TestEloadIngestion


class TestEloadIngestionResume(TestEloadIngestion):

    def test_resume_when_step_fails(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_run_command, \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            browsable_files = [(1, 'ERA', 'filename_1', 'PRJ', 123), (2, 'ERA', 'filename_1', 'PRJ', 123)]
            m_get_results.side_effect = [
                [(391,)],  # Check the assembly_set_id in update_assembly_set_in_analysis
                browsable_files,  # insert_browsable_files files_query
                browsable_files,  # insert_browsable_files find_browsable_files_query
                [(1, 'filename_1'), (2, 'filename_2')],  # update_files_with_ftp_path
                [('Test Study Name')],  # get_study_name
                [(1, 'filename_1'), (2, 'filename_2')]  # update_loaded_assembly_in_browsable_files
            ]
            m_run_command.side_effect = [
                None,  # metadata load
                CalledProcessError(1, 'nextflow accession'),  # accession
                CalledProcessError(2, 'nextflow accession'),  # fails twice
                None,  # accession on resume
                None,  # variant load
            ]

            # Accession process fails twice before succeeding
            with self.assertRaises(CalledProcessError):
                self.eload.ingest()
            assert self.eload.get_step() == SubmissionStep.ACCESSION
            nextflow_dir = self.eload.eload_cfg.query(self.eload.config_section, 'accession', 'nextflow_dir')
            assert os.path.exists(nextflow_dir)

            with self.assertRaises(CalledProcessError):
                self.eload.ingest(resume=True)
            assert self.eload.get_step() == SubmissionStep.ACCESSION
            assert os.path.exists(nextflow_dir)

            self.eload.ingest(resume=True)
            assert self.eload.get_step() is None
            assert not os.path.exists(nextflow_dir)

    def test_resume_completed_job(self):
        with self._patch_metadata_handle(), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version') as m_get_vep_versions, \
                patch('eva_submission.eload_utils.requests.post') as m_post, \
                self._patch_mongo_database():
            m_get_alias_results.return_value = [['alias']]
            m_get_vep_versions.return_value = (100, 100, 'homo_sapiens')
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            browsable_files = [(1, 'ERA', 'filename_1', 'PRJ', 123), (2, 'ERA', 'filename_1', 'PRJ', 123)]

            # Cycle through these results so we can call ingest multiple times
            m_get_results.side_effect = cycle([
                [(391,)],  # Check the assembly_set_id in update_assembly_set_in_analysis
                browsable_files,  # insert_browsable_files files_query
                browsable_files,  # insert_browsable_files find_browsable_files_query
                [(1, 'filename_1'), (2, 'filename_2')],  # update_files_with_ftp_path
                [('Test Study Name')],  # get_study_name
                [(1, 'filename_1'), (2, 'filename_2')]  # update_loaded_assembly_in_browsable_files
            ])

            # Resuming with no existing job execution is fine
            self.eload.ingest(resume=True)
            num_calls = m_get_results.call_count

            # Note that if we resume a completed job, everything will re-run.
            # To avoid this we would need to store a job status as well as current step.
            self.eload.ingest(resume=True)
            assert m_get_results.call_count == 2*num_calls
