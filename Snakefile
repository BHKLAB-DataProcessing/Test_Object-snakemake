from os import path
from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
S3 = S3RemoteProvider(
    access_key_id=config["key"],
    secret_access_key=config["secret"],
    host=config["host"],
    stay_on_remote=False
)

prefix = config["prefix"]
filename = config["filename"]
is_filtered = config["filtered"]
filtered = 'filtered' if config["filtered"] is not None and config["filtered"] == 'filtered' else ''

rule get_fimm:
    input:
        S3.remote(prefix + "processed/profiles.RData"),
        S3.remote(prefix + "processed/drug.info.rds"),
        S3.remote(prefix + "processed/cell.info.rds"),
        S3.remote(prefix + "processed/curationCell.rds"),
        S3.remote(prefix + "processed/curationDrug.rds"),
        S3.remote(prefix + "processed/curationTissue.rds"),
        S3.remote(prefix + "processed/sens.info.rds"),
        S3.remote(prefix + "processed/sens.prof.rds"),
        S3.remote(prefix + "processed/sens.raw.rds"),
        S3.remote(prefix + "download/drugs_with_ids.csv"),
        S3.remote(prefix + "download/cell_annotation_all.csv"),
    output:
        S3.remote(prefix + filename)
    shell:
        """
        Rscript scripts/getFIMM.R {prefix} {filename} {filtered}
        """

rule recalculate_and_assemble:
    input:
        S3.remote(prefix + "processed/raw_sense_slices.zip"),
    output:
        S3.remote(prefix + "processed/profiles.RData")
    shell:
        """
        Rscript scripts/recalculateAndAssemble.R {prefix}
        """

rule process_fimm:
    input:
        S3.remote(prefix + "download/drugs_with_ids.csv"),
        S3.remote(prefix + "download/cell_annotation_all.csv"),
        S3.remote(prefix + "download/41586_2016_BFnature20171_MOESM60_ESM.xls"),
        S3.remote(prefix + "download/41586_2016_BFnature20171_MOESM61_ESM.xlsx")
    output:
        S3.remote(prefix + "processed/drug.info.rds"),
        S3.remote(prefix + "processed/cell.info.rds"),
        S3.remote(prefix + "processed/curationCell.rds"),
        S3.remote(prefix + "processed/curationDrug.rds"),
        S3.remote(prefix + "processed/curationTissue.rds"),
        S3.remote(prefix + "processed/sens.info.rds"),
        S3.remote(prefix + "processed/sens.prof.rds"),
        S3.remote(prefix + "processed/sens.raw.rds"),
        S3.remote(prefix + "processed/raw_sense_slices.zip")
    shell:
        """
        Rscript scripts/processFIMM.R {prefix}
        """

rule download_annotation:
    output:
        S3.remote(prefix + "download/drugs_with_ids.csv"),
        S3.remote(prefix + "download/cell_annotation_all.csv")
    shell:
        """
        wget 'https://github.com/BHKLAB-DataProcessing/Annotations/raw/master/drugs_with_ids.csv' \
            -O {prefix}download/drugs_with_ids.csv
        wget 'https://github.com/BHKLAB-DataProcessing/Annotations/raw/master/cell_annotation_all.csv' \
            -O {prefix}download/cell_annotation_all.csv
        """

rule download_data:
    output:
        S3.remote(prefix + "download/41586_2016_BFnature20171_MOESM60_ESM.xls"),
        S3.remote(prefix + "download/41586_2016_BFnature20171_MOESM61_ESM.xlsx")
    shell:
        """
        wget 'https://static-content.springer.com/esm/art%3A10.1038%2Fnature20171/MediaObjects/41586_2016_BFnature20171_MOESM60_ESM.xls' \
            -O {prefix}download/41586_2016_BFnature20171_MOESM60_ESM.xls
        wget 'https://static-content.springer.com/esm/art%3A10.1038%2Fnature20171/MediaObjects/41586_2016_BFnature20171_MOESM61_ESM.xlsx' \
            -O {prefix}download/41586_2016_BFnature20171_MOESM61_ESM.xlsx
        """
