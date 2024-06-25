rule make_pileups:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        features=lambda wildcards: bedfiles_dict[wildcards.features],
        expected=lambda wildcards: (
            f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv"
            if wildcards.norm == "expected"
            else []
        ),
        view=lambda wildcards: config["view"],
    output:
        f"{pileups_folder}/{{folder}}/{{sample}}-{{resolution,[0-9]+}}_over_{{features}}_{{norm}}_{{extra,.*}}.clpy",
    benchmark:
        "benchmarks/make_pileups/{folder}/{sample}-{resolution,[0-9]+}_over_{features}_{norm}_{extra,.*}.tsv"
    log:
        "logs/make_pileups/{folder}/{sample}-{resolution,[0-9]+}_over_{features}_{norm}_{extra,.*}.tsv",
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
        sample="|".join(samples),
    params:
        features_format=lambda wildcards: bedtype_dict[wildcards.features],
        extra=lambda wildcards: f"{pileup_params[wildcards.extra]} {get_shifts(wildcards.norm)} -l ERROR",
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    wrapper:
        "v3.12.2/bio/coolpuppy"


rule plot_pileups_individual:
    input:
        pileups=f"{pileups_folder}/{{folder}}/{{sample}}-{{resolution}}_over_{{features}}_{{norm}}_{{extra}}.clpy",
    output:
        report(
            f"{pileups_folder}/figures/{{folder}}/{{sample}}_{{features}}_individual-{{resolution}}_{{norm}}_{{extra,.*}}.{{ext}}",
            caption="../report/pileups_individual.rst",
            category="Pileups",
            subcategory="{folder}",
        ),
    log:
        "logs/plot_pileups_individual/{sample}_{folder}_{resolution}_{features}_{norm}_{extra}_{ext}.log",
    benchmark:
        "benchmarks/plot_pileups_individual/{sample}_{folder}_{resolution}_{features}_{norm}_{extra}_{ext}.tsv"
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
        sample="|".join(samples),
    threads: 1
    resources:
        mem_mb=lambda wildcards, threads: threads * 1024,
        runtime=24 * 60,
    conda:
        "../envs/coolpuppy_env.yml"
    params:
        extra=lambda wildcards: {
            config["pileups"]["arguments"][wildcards.extra]["plot"]["individual"]
        },
    shell:
        """
        plotpup.py --quaich --height 1.5 {params.extra} --output {output} --input_pups {input.pileups} >{log[0]} 2>&1
        """


rule plot_pileups_compare_samples:
    input:
        pileups=lambda wildcards: expand(
            f"{pileups_folder}/{{folder}}/{{sample}}-{{resolution}}_over_{{features}}_{{norm}}_{{extra}}.clpy",
            sample=samples_to_use.get(wildcards.features, samples),
            allow_missing=True,
        ),
    output:
        report(
            f"{pileups_folder}/figures/{{folder}}/{{features}}_compare_samples-{{resolution}}_{{norm}}_{{extra,.*}}.{{ext}}",
            caption="../report/pileups_by_sample.rst",
            category="Pileups",
            subcategory="{folder}",
        ),
    log:
        "logs/plot_pileups_compare_samples/{folder}_{features}_compare_samples-{resolution}_{norm}_{extra,.*}.{ext}.log",
    benchmark:
        "benchmarks/plot_pileups_compare_samples/{folder}_{features}_compare_samples-{resolution}_{norm}_{extra,.*}.{ext}.tsv"
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
    threads: 1
    resources:
        mem_mb=lambda wildcards, threads: threads * 1024,
        runtime=24 * 60,
    conda:
        "../envs/coolpuppy_env.yml"
    params:
        extra=lambda wildcards: {
            config["pileups"]["arguments"][wildcards.extra]["plot"]["compare_samples"]
        },
    shell:
        """
        plotpup.py --quaich --height 1.5 {params.extra} --output {output} --input_pups {input.pileups} >{log[0]} 2>&1
        """
