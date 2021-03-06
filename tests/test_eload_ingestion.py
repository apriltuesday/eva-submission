import glob
import os
import shutil
import subprocess
from copy import deepcopy
from unittest import TestCase, mock
from unittest.mock import patch

import yaml

from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config


class TestEloadIngestion(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        with patch('eva_submission.eload_ingestion.get_mongo_uri_for_eva_profile', autospec=True):
            self.eload = EloadIngestion(33)
        # Used to restore test config after each test
        self.original_cfg = deepcopy(self.eload.eload_cfg.content)

    def tearDown(self):
        projects = glob.glob(os.path.join(self.resources_folder, 'projects', 'PRJEB12345'))
        for proj in projects:
            shutil.rmtree(proj)
        ingest_csv = os.path.join(self.eload.eload_dir, 'vcf_files_to_ingest.csv')
        if os.path.exists(ingest_csv):
            os.remove(ingest_csv)
        self.eload.eload_cfg.content = self.original_cfg

    def _mock_mongodb_client(self):
        m_db = mock.Mock()
        m_db.list_database_names = mock.Mock(return_value=[
            'eva_ecaballus_30',
            'eva_hsapiens_grch38'
        ])
        return m_db

    def test_check_brokering_done(self):
        self.eload.project_accession = None
        with self.assertRaises(ValueError):
            self.eload.check_brokering_done()
        del self.eload.eload_cfg.content['brokering']
        with self.assertRaises(ValueError):
            self.eload.check_brokering_done()

    def test_check_variant_db(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_variant_warehouse_db_name_from_assembly_and_taxonomy',
                      autospec=True) as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_results.return_value = 'eva_ecaballus_30'
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()

            self.eload.check_variant_db()
            self.assertEqual(
                'eva_ecaballus_30',
                self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'db_name')
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'exists')

    def test_check_variant_db_not_in_evapro(self):
        with patch('eva_submission.eload_ingestion.get_variant_warehouse_db_name_from_assembly_and_taxonomy',
                   autospec=True) as m_get_results, \
                patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_results.return_value = None
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            with self.assertRaises(ValueError):
                self.eload.check_variant_db()

    def test_check_variant_db_name_provided(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.check_variant_db(db_name='eva_hsapiens_grch38')
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'db_name'),
                'eva_hsapiens_grch38'
            )
            assert self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'exists')

    def test_check_variant_db_missing(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo:
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()

            with self.assertRaises(ValueError):
                self.eload.check_variant_db(db_name='eva_fcatus_90')
            self.assertEqual(
                self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'db_name'),
                'eva_fcatus_90'
            )
            assert not self.eload.eload_cfg.query('ingestion', 'database', 'GCA_002863925.1', 'exists')

    def test_load_from_ena(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_load_from_ena_script_fails(self):
        with patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True) as m_execute:
            m_execute.side_effect = subprocess.CalledProcessError(1, 'some command')
            with self.assertRaises(subprocess.CalledProcessError):
                self.eload.load_from_ena()
            m_execute.assert_called_once()

    def test_ingest_all_tasks(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_primary_mongo_creds_for_profile',
                      autospec=True) as m_mongo_creds, \
                patch('eva_submission.eload_ingestion.get_accession_pg_creds_for_profile',
                      autospec=True) as m_pg_creds, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_mongo_creds.return_value = m_pg_creds.return_value = ('host', 'user', 'pass')
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            # first call is for browsable files, second is for study name
            m_get_results.side_effect = [[(1, 'filename_1'), (2, 'filename_2')], [('Test Study Name')]]
            self.eload.ingest('NONE', 1, 82, 82, db_name='eva_hsapiens_grch38')

    def test_ingest_metadata_load(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            self.eload.ingest(tasks=['metadata_load'], db_name='eva_hsapiens_grch38')

    def test_ingest_accession(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_primary_mongo_creds_for_profile',
                      autospec=True) as m_mongo_creds, \
                patch('eva_submission.eload_ingestion.get_accession_pg_creds_for_profile', autospec=True) as m_pg_creds, \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_mongo_creds.return_value = m_pg_creds.return_value = ('host', 'user', 'pass')
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [(1, 'filename_1'), (2, 'filename_2')]
            self.eload.ingest(
                aggregation='NONE',
                instance_id=1,
                tasks=['accession'],
                db_name='eva_hsapiens_grch38'
            )
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/accession_config_file.yaml')
            )

    def test_ingest_variant_load(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            self.eload.ingest(
                aggregation='NONE',
                vep_version=82,
                vep_cache_version=82,
                tasks=['variant_load'],
                db_name='eva_hsapiens_grch38'
            )
            assert os.path.exists(
                os.path.join(self.resources_folder, 'projects/PRJEB12345/load_config_file.yaml')
            )

    def test_insert_browsable_files(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.execute_query') as m_execute:
            m_get_results.side_effect = [[], [(1, 'filename_1'), (2, 'filename_2')], [(1, 'filename_1'), (2, 'filename_2')]]
            self.eload.insert_browsable_files()
            m_execute.assert_called()

            # calling insert again doesn't execute anything
            m_execute.call_count = 0
            self.eload.insert_browsable_files()
            m_execute.assert_not_called()

    def get_mock_result_for_ena_date(self):
        return '''<?xml version="1.0" encoding="UTF-8"?>
            <?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
            <RECEIPT receiptDate="2021-04-19T18:37:45.129+01:00" submissionFile="SUBMISSION" success="true">
                 <ANALYSIS accession="ERZ999999" alias="MD" status="PRIVATE"/>
                 <PROJECT accession="PRJEB12345" alias="alias" status="PRIVATE" holdUntilDate="2021-01-01+01:00"/>
                 <SUBMISSION accession="ERA3972426" alias="alias"/>
                 <MESSAGES/>
                 <ACTIONS>RECEIPT</ACTIONS>
            </RECEIPT>'''

    def test_ingest_variant_load_vep_cache_version_provided_by_user(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            self.eload.ingest(
                aggregation='NONE',
                tasks=['variant_load'],
                vep_version=100,
                vep_cache_version=100,
                skip_annotation=False,
                db_name='eva_hsapiens_grch38'
            )
            config_file = os.path.join(self.resources_folder, 'projects/PRJEB12345/load_config_file.yaml')
            assert os.path.exists(config_file)
            with open(config_file, 'r') as stream:
                data_loaded = yaml.safe_load(stream)
                self.assertEqual(data_loaded["load_job_props"]['annotation.skip'], False)
                self.assertEqual(data_loaded["load_job_props"]['app.vep.version'], 100)
                self.assertEqual(data_loaded["load_job_props"]['app.vep.cache.version'], 100)

    def test_ingest_variant_load_vep_cache_version_found_in_db(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version_from_db') as get_vep_and_vep_cache_version_from_db, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            get_vep_and_vep_cache_version_from_db.return_value = {"vep_version": 100, "vep_cache_version": 100}
            self.eload.ingest(
                aggregation='NONE',
                tasks=['variant_load'],
                vep_version=None,
                vep_cache_version=None,
                skip_annotation=False,
                db_name='eva_hsapiens_grch38'
            )
            config_file = os.path.join(self.resources_folder, 'projects/PRJEB12345/load_config_file.yaml')
            assert os.path.exists(config_file)
            with open(config_file, 'r') as stream:
                data_loaded = yaml.safe_load(stream)
                self.assertEqual(data_loaded["load_job_props"]['annotation.skip'], False)
                self.assertEqual(data_loaded["load_job_props"]['app.vep.version'], 100)
                self.assertEqual(data_loaded["load_job_props"]['app.vep.cache.version'], 100)

    def test_ingest_variant_load_vep_cache_version_not_found_in_db(self):
        with patch('eva_submission.eload_submission.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_ingestion.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_ingestion.pymongo.MongoClient', autospec=True) as m_get_mongo, \
                patch('eva_submission.eload_ingestion.command_utils.run_command_with_output', autospec=True), \
                patch('eva_submission.eload_utils.get_metadata_connection_handle', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_ingestion.get_vep_and_vep_cache_version_from_db') as get_vep_and_vep_cache_version_from_db, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_post.return_value.text = self.get_mock_result_for_ena_date()
            m_get_mongo.return_value.__enter__.return_value = self._mock_mongodb_client()
            m_get_results.return_value = [('Test Study Name')]
            get_vep_and_vep_cache_version_from_db.return_value = {"vep_version": None, "vep_cache_version": None}
            with self.assertRaises(Exception) as ex:
                self.eload.ingest(
                    aggregation='NONE',
                    tasks=['variant_load'],
                    vep_version=None,
                    vep_cache_version=None,
                    skip_annotation=False,
                    db_name='eva_hsapiens_grch38'
                )
            self.assertEqual(ex.exception.__str__(), 'No vep_version and vep_cache_version provided by user and none could be found in DB.'
                                                     'In case you want to process without annotation, please use --skip_annotation parameter.')
