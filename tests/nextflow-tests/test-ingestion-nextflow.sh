#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/nextflow"

echo Script dir: $SCRIPT_DIR
echo Source dir: $SOURCE_DIR

nextflow run ${SOURCE_DIR}/accession.nf \
-params-file ${SCRIPT_DIR}/resources/test_ingestion_config.yaml

# clean up
rm -rf work .nextflow*
