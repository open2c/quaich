rule make_expected_cis:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
    output:
        f"{expected_folder}/{{sample}}_{{resolution,[0-9]+}}.expected.tsv",
    benchmark:
        "benchmarks/make_expected_cis/{sample}_{resolution,[0-9]+}.expected.cis.tsv"
    log:
        "logs/make_expected_cis/{sample}_{resolution,[0-9]+}.expected.cis.tsv",
    params:
        extra=lambda wildcards: config["expected"]["extra_args_cis"],
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 8 * 1024,
        runtime=60,
    wrapper:
        "v3.12.2/bio/cooltools/expected_cis"


rule make_expected_trans:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
    output:
        f"{expected_folder}/{{sample}}_{{resolution,[0-9]+}}.expected.trans.tsv",
    benchmark:
        "benchmarks/make_expected_trans/{sample}_{resolution,[0-9]+}.expected.trans.tsv"
    log:
        "logs/make_expected_trans/{sample}_{resolution,[0-9]+}.expected.trans.tsv",
    params:
        extra=lambda wildcards: config["expected"]["extra_args_trans"],
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 8 * 1024,
        runtime=60,
    wrapper:
        "v3.12.2/bio/cooltools/expected_trans"
