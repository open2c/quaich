rule make_compartments:
    input:
        bigwig=f"{eig_profiles_folder}/{{sample}}_{{resolution}}_eigenvectors.{{mode}}.bw",
        view=lambda wildcards: config["view"],
    output:
        compartments=f"{compartments_folder}/{{sample}}_{{resolution,[0-9]+}}_compartments.{{mode}}.bed",
    benchmark:
        "benchmarks/make_compartments/{sample}_{resolution,[0-9]+}_compartments.{mode}.tsv"
    log:
        "logs/make_compartments/{sample}_{resolution,[0-9]+}_compartments.{{mode}.tsv",
    threads: 1
    resources:
        mem_mb=1 * 1024,
        runtime=10,
    conda:
        "../envs/hmm_bigwigs_env.yml"
    shell:
        f"bigwig_hmm.py -i {{input.bigwig}} --view {{input.view}} -n 2 -o {{output.compartments}} >{log[0]} 2>&1"


rule make_eigenvectors_cis:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
        track=lambda wildcards: path.join(
            config["path_genome_folder"],
            "gc/",
            f"{genome}_{{resolution}}_gc.bedgraph",
        ),
    output:
        vecs=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.cis.vecs.tsv",
        lam=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.cis.lam.txt",
        bigwig=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.cis.bw",
    benchmark:
        "benchmarks/make_eigenvectors_cis/{sample}_{resolution,[0-9]+}_eigenvectors.cis.tsv"
    log:
        "logs/make_eigenvectors_cis/{sample}_{resolution,[0-9]+}_eigenvectors.cis.tsv",
    params:
        extra="--bigwig",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:        "v3.12.2/bio/cooltools/eigs_cis"


rule make_eigenvectors_trans:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        track=lambda wildcards: path.join(
            config["path_genome_folder"],
            "gc/",
            f"{genome}_{{resolution}}_gc.bedgraph",
        ),
    output:
        vecs=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.trans.vecs.tsv",
        lam=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.trans.lam.txt",
        bigwig=f"{eig_profiles_folder}/{{sample}}_{{resolution,[0-9]+}}_eigenvectors.trans.bw",
    benchmark:
        "benchmarks/make_eigenvectors_trans/{sample}_{resolution,[0-9]+}_eigenvectors.trans.tsv"
    log:
        "logs/make_eigenvectors_trans/{sample}_{resolution,[0-9]+}_eigenvectors.trans.tsv",
    params:
        track_name_col="GC",
        extra="",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v3.12.2/bio/cooltools/eigs_trans"


rule make_gc:
    input:
        fasta=config["path_genome_fasta"],
        bins=lambda wildcards: path.join(
            f'{config["path_genome_folder"]}',
            "bins/",
            f"{genome}_{{resolution}}_bins.bed",
        ),
    output:
        path.join(
            config["path_genome_folder"],
            "gc/",
            f"{genome}_{{resolution,[0-9]+}}_gc.bedgraph",
        ),
    log:
        "logs/make_gc/{resolution}.log",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v3.12.2/bio/cooltools/genome/gc"


rule make_bins:
    input:
        chromsizes=config["chromsizes"],
    output:
        path.join(
            config["path_genome_folder"],
            "bins/",
            f"{genome}_{{resolution,[0-9]+}}_bins.bed",
        ),
    log:
        "logs/make_bins/{resolution}.log",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    params:
        binsize=lambda wildcards: wildcards["resolution"],
    wrapper:
        "v3.12.2/bio/cooltools/genome/binnify"
