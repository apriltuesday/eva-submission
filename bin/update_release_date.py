#!/usr/bin/env python
# Copyright 2026 EMBL - European Bioinformatics Institute
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

import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from requests.auth import HTTPBasicAuth
from retry import retry

from eva_sub_cli_processing.sub_cli_utils import (
    put_to_sub_ws, sub_ws_url_build, fetch_submission_from_eload, fetch_submission
)
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


@retry(requests.exceptions.ConnectionError, tries=3, delay=2, backoff=1.2, jitter=(1, 3))
def update_ena_release_date(project_accession, release_date):
    body = {
        "submission": {
            "accession": project_accession,
            "actions": [
                {
                    "type": "HOLD",
                    "target": project_accession,
                    "holdUntilDate": release_date
                }
            ]
        }
    }
    ena_auth = HTTPBasicAuth(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
    ena_url = cfg.query('ena', 'submit_url')
    response = requests.post(ena_url, auth=ena_auth, data=body, headers={'Accept': 'application/json'})
    response.raise_for_status()

    # Check for error messages in ENA receipt
    try:
        receipt = response.json()
        messages = receipt.get('messages')
        if 'error' in messages:
            logger.error(f'Failed to update release date for {project_accession} on ENA. Error: {messages["error"]}')
            sys.exit(1)
        else:
            logger.info(f'Updated release date for {project_accession} on ENA')
    except Exception as e:
        logger.error(f'Failed to update release date for {project_accession} on ENA. Error: {e}')
        sys.exit(1)

def main():
    argparse = ArgumentParser(description='Update the release date of a submission in the submission webservice')
    target = argparse.add_mutually_exclusive_group(required=True)
    target.add_argument('--submission_id', required=False, type=str,
                        help='Target submission by UUID')
    target.add_argument('--eload_id', required=False, type=int,
                        help='Target submission by ELOAD number (resolved to UUID via API)')
    argparse.add_argument('--release_date', required=True, type=str,
                          help='Release date in ISO-8601 format (e.g. 2027-01-31)')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    if args.eload_id:
        submission = fetch_submission_from_eload(args.eload_id)
        if not submission:
            logger.error(f'No submission found for ELOAD {args.eload_id}')
            sys.exit(1)
        submission_id = submission['submissionId']
        logger.info(f'Resolved ELOAD {args.eload_id} to submission {submission_id}')
    else:
        submission_id = args.submission_id
        submission = fetch_submission(submission_id)
        if not submission:
            logger.error(f'Submission {submission_id} not found')
            sys.exit(1)

    project_accession = submission.get('projectAccession')
    old_release_date = submission.get('releaseDate') or 'None'

    if not project_accession:
        logger.error(f'Could not determine project accession for submission {submission_id}')
        sys.exit(1)
    update_ena_release_date(project_accession, args.release_date)
    put_to_sub_ws(sub_ws_url_build('admin', 'submission', submission_id, 'trackingDetails'),
                  {'releaseDate': args.release_date})
    logger.info(f'Updated submission {submission_id}: {old_release_date} -> {args.release_date}')


if __name__ == '__main__':
    main()
