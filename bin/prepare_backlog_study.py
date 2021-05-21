#!/usr/bin/env python

# Copyright 2021 EMBL - European Bioinformatics Institute
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
import os
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from eva_submission.eload_backlog import EloadBacklog
from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Prepare to process backlog study and validate VCFs.')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--analysis', required=False,
                          help='The analysis accession to use, if different from the one already in metadata db.')
    argparse.add_argument('--vcf_file', required=False,
                          help='The VCF file to use, if different from the one already in metadata db.')
    argparse.add_argument('--index_file', required=False,
                          help='The index file to use, if different from the one already in metadata db.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()
    # verify all necessary arguments present if analysis is being passed in
    if any((args.analysis, args.vcf_file, args.index_file)):
        assert all((args.analysis, args.vcf_file, args.index_file)), \
            'Include analysis accession, vcf file path, and index file path if you want to use a specific analysis.'

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    preparation = EloadBacklog(args.eload)
    preparation.fill_in_config(analysis=args.analysis, vcf_file=args.vcf_file, index_file=args.index_file)
    preparation.report()

    validation = EloadValidation(args.eload)
    validation_tasks = ['assembly_check', 'vcf_check']
    validation.validate(validation_tasks)

    logger.info('Preparation complete, if files are valid please run ingestion as normal.')


if __name__ == "__main__":
    main()
