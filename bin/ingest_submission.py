#!/usr/bin/env python

# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from argparse import ArgumentParser, ArgumentError

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Accession and ingest submission data into EVA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission.')
    argparse.add_argument('--instance', required=False, type=int, choices=range(1, 13),
                          help='The instance id to use for accessioning. Only needed if running accessioning.')
    # TODO infer aggregation from vcf files, VEP version & cache version from species
    argparse.add_argument('--aggregation', required=False, type=str.lower, choices=['basic', 'none'],
                          help='The aggregation type (case insensitive).')
    action_vep_version = argparse.add_argument('--vep_version', required=False, type=int,
                                               help='VEP version to use for annotation. Only needed if running variant load.')
    argparse.add_argument('--vep_cache_version', required=False, type=int,
                          help='VEP cache version to use for annotation. Only needed if running variant load.')
    argparse.add_argument('--db_name', required=False, type=str,
                          help='Name of an existing variant database in MongoDB. Submission should have a single '
                               'assembly accession. Only needed if adding a new database. ex: db_name')
    argparse.add_argument('--db_name_mapping', required=False, type=str, nargs='+',
                          help='List with the mapping for assembly accession and existing variant database in MongoDB.'
                               'Only needed if adding a new databases.'
                               'ex: GCA_000000001.1,db_name1 GCA_000000002.2,db_name2')
    argparse.add_argument('--tasks', required=False, type=str, nargs='+',
                          default=EloadIngestion.all_tasks, choices=EloadIngestion.all_tasks,
                          help='Task or set of tasks to perform during ingestion.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level.')
    action_skip_annotation = argparse.add_argument('--skip_annotation', action='store_true', default=False,
                                                   help='Flag to skip VEP annotation running variant load.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    if args.skip_annotation is True and (args.vep_version is not None or args.vep_cache_version is not None):
        raise ArgumentError(action_skip_annotation,
                            "Can't provide both \"--skip_annotation\" and \"--vep_version and --vep_cache_version\". Remove VEP/Cache versions or the skip flag and try again.")
    if (args.vep_version is None and args.vep_cache_version is not None) or (
            args.vep_version is not None and args.vep_cache_version is None):
        raise ArgumentError(action_vep_version,
                            "Both \"--vep_version and --vep_cache_version\" should be specified together. Skip both arguments for auto-detection of these versions.")

    # Load the config_file from default location
    load_config()

    ingestion = EloadIngestion(args.eload)
    ingestion.upgrade_config_if_needed()
    ingestion.ingest(
        aggregation=args.aggregation,
        instance_id=args.instance,
        vep_version=args.vep_version,
        vep_cache_version=args.vep_cache_version,
        skip_annotation=args.skip_annotation,
        db_name=args.db_name,
        db_name_mapping=args.db_name_mapping,
        tasks=args.tasks
    )


if __name__ == "__main__":
    main()
