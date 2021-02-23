#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Accession variant files and copy to public FTP.

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
accessioned_vcfs = Channel.watchPath(params.public_dir + '/*.vcf')


/*
 * Accession VCF
 */
process accession_vcf {
    clusterOptions '-g /accession/instance-$params.instance_id'

    input:
        path accession_properties from accession_props

    """
    filename=\$(basename $accession_properties)
    filename=\${filename%.*}
    java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=$accession_properties \
        > $params.logs_dir/accessioning.\${filename}.log \
        2> $params.logs_dir/accessioning.\${filename}.err
    """
}


/*
 * Compress accessioned VCFs
 */
process compress_vcfs {
    input:
        path vcf_file from accessioned_vcfs

    output:
        path "${vcf_file}.gz" into compressed_vcf1
        path "${vcf_file}.gz" into compressed_vcf2

    """
    $params.executable.bgzip -c $vcf_file > ${vcf_file}.gz
    """
}


/*
 * Index the compressed VCF file
 */
process tabix_index_vcf {
    input:
        path compressed_vcf from compressed_vcf1

    output:
        path "${compressed_vcf}.tbi" into tbi_indexed_vcf

    """
    $params.executable.tabix -p vcf $compressed_vcf
    """
}


process csi_index_vcf {
    input:
        path compressed_vcf from compressed_vcf2

    output:
        path "${compressed_vcf}.csi" into csi_indexed_vcf

    """
    $params.executable.bcftools index -c $compressed_vcf
    """
}


/*
 * Move files from eva_public to FTP folder.
 */
 process move_to_ftp {
    // TODO this needs to really wait for everything, not just one file from each
    input:
        path _ from csi_indexed_vcf
        path _ from tbi_indexed_vcf

    """
    cd $params.public_dir
    $params.executable.copy_to_ftp $params.project_accession
    """
 }
