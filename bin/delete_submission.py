#!/usr/bin/env python

# Copyright 2025 EMBL - European Bioinformatics Institute
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
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing.sub_cli_utils import fetch_submission
from eva_submission.eload_deletion import EloadDeletion
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Delete/Archive submission')
    target = argparse.add_mutually_exclusive_group(required=True)
    target.add_argument('--submission_id', required=False, type=str,
                        help='Submission ID, converted to ELOAD for downstream processing')
    target.add_argument('--eload', required=False, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--ftp_box', required=False, type=int, choices=range(1, 21), default=None,
                          help='box number where the data has been uploaded')
    argparse.add_argument('--submitter', required=False, type=str, help='the name of the directory for the submitter')
    argparse.add_argument('-f', '--force_delete', action="store_true",
                          help='force delete the existing eload tar file in LTS directory')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    if args.submission_id:
        submission = fetch_submission(args.submission_id)
        if not submission:
            logger.error(f'Submission {args.submission_id} not found')
            sys.exit(1)
        eload_id = submission.get('eloadId')
    else:
        eload_id = args.eload

    # Do NOT use context manager to ensure the Eload object does not rewrite the config after deletion!
    submission_deletion = EloadDeletion(eload_id)
    submission_deletion.delete_submission(args.ftp_box, args.submitter, args.force_delete)


if __name__ == "__main__":
    main()
