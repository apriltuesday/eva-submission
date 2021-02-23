#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}
mkdir project project/logs project/public

# run accession and variant load
# note public_dir needs to be an absolute path, unlike others in config
nextflow run ${SOURCE_DIR}/accession.nf -params-file test_ingestion_config.yaml \
	 --public_dir ${SCRIPT_DIR}/project/public
nextflow run ${SOURCE_DIR}/variant_load.nf -params-file test_ingestion_config.yaml

# check for public files and logs
printf "====== Files made public ======\n"
ls ${SCRIPT_DIR}/project/public
printf "\n======== Java commands ========\n"
cat ${SCRIPT_DIR}/project/logs/*.log

# clean up
rm -rf work .nextflow*
rm -r project
cd ${cwd}
