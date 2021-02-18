#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Accession and ingest variant files.

    Inputs:
            --accession_props       properties files for accessioning
            --project_accession     project accession
            --instance_id           instance id to run accessioning
    """
}

params.accession_props = null
params.project_accession = null
params.instance_id = null
// executables
params.executable = ["bgzip": "bgzip", "tabix": "tabix", "copy_to_ftp": "copy_to_ftp"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.accession_props || !params.project_accession || !params.instance_id) {
    if (!params.accession_props)    log.warn('Provide an accessions properties file using --accession_props')
    if (!params.project_accession)  log.warn('Provide a project accession using --project_accession')
    if (!params.instance_id)        log.warn('Provide an instance id using --instance_id')
    exit 1, helpMessage()
}

accession_props = Channel.fromPath(params.accession_props)


/*
* Accession VCF
*/
process accession_vcf {
    clusterOptions '-g /accession/instance-$params.instance_id'

    input:
        path "accession.properties" from accession_props

    output:
        path "00_logs/accessioning.*.log" into accessioning_log
        path "00_logs/accessioning.*.err" into accessioning_err
        path "60_eva_public/*.vcf" into accessioned_vcfs

    """
    filename=$(basename accession.properties)
    filename="${filename%.*}"
    java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=accession.properties \
        > 00_logs/accessioning.${filename}.log \
        2> 00_logs/accessioning.${filename}.err
    """
}


/*
 * Compress accessioned VCFs
 */
process compress_vcfs {
    input:
        path vcf_file from accessioned_vcfs

    output:
        path "60_eva_public/*.gz" into compressed_vcfs

    """
    $params.executable.bgzip -c $vcf_file > ${vcf_file}.gz
    """
}


/*
* Index the compressed VCF file
*/
process index_vcf {
    input:
        path compressed_vcf from compressed_vcfs

    output:
        path "${compressed_vcf}.tbi" into indexed_vcfs

    """
    $params.executable.tabix -p vcf $compressed_vcf
    """
}


/*
 * Move files from eva_public to FTP folder.
 */
 process move_to_ftp {
    input:
        path _ from indexed_vcfs

    """
    cd 60_eva_public
    $params.executable.copy_to_ftp $params.project_accession
    cd ..
    """
 }