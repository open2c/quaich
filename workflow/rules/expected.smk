rule make_expected_cis:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
    output:
        f"{expected_folder}/{{sample}}_{{resolution,[0-9]+}}.expected.tsv",
    params:
        extra=lambda wildcards: config["expected"]["extra_args_cis"],
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 8 * 1024,
        runtime=60,
    wrapper:
        "v1.21.2/bio/cooltools/expected_cis"


rule make_expected_trans:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
    output:
        f"{expected_folder}/{{sample}}_{{resolution,[0-9]+}}.expected.trans.tsv",
    params:
        extra=lambda wildcards: config["expected"]["extra_args_trans"],
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 8 * 1024,
        runtime=60,
    wrapper:
        "v1.21.2/bio/cooltools/expected_trans"


rule make_bins:
    input:
        chromsizes=config["chromsizes"],
    output:
        path.join(
            config["path_genome_folder"],
            "bins/",
            f"{genome}_{{resolution,[0-9]+}}_bins.bed",
        ),
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    params:
        binsize=lambda wildcards: wildcards["resolution"],
    wrapper:
        "v1.21.2/bio/cooltools/genome/binnify"
