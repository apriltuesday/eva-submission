#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

nextflow run ${SOURCE_DIR}/accession.nf -params-file test_ingestion_config.yaml

# nextflow run ${SOURCE_DIR}/variant_load.nf -params-file test_ingestion_config.yaml

# clean up
rm -rf work .nextflow*
cd ${cwd}
