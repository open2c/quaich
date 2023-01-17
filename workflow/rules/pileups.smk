rule make_pileups:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        features=lambda wildcards: bedfiles_dict[wildcards.features],
        expected=lambda wildcards: f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv"
        if wildcards.norm == "expected"
        else [],
        view=lambda wildcards: config["view"],
    output:
        f"{eigenvectors_folder}/pentads/{{sample}}-{{resolution,[0-9]+}}_{{norm}}_{{extra,.*}}.clpy",
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
    params:
        features_format=lambda wildcards: bedtype_dict[wildcards.features],
        extra=lambda wildcards: f"{pileup_params[wildcards.extra]} {get_shifts(wildcards.norm)}",
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    wrapper:
        "v1.21.2/bio/coolpuppy"