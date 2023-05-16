rule make_compartments:
    input:
        bigwig=f"{eigenvectors_folder}/{eig_profiles_folder}/{{sample}}_{{resolution}}_eigenvectors.{{mode}}.bw",
        view=lambda wildcards: config["view"],
    output:
        compartments=f"{eigenvectors_folder}/compartments/{{sample}}_{{resolution,[0-9]+}}_compartments.{{mode}}.bed",
    threads: 1
    resources:
        mem_mb=1 * 1024,
        runtime=10,
    conda:
        "../envs/hmm_bigwigs_env.yml"
    shell:
        f"bigwig_hmm.py -i {{input.bigwig}} --view {{input.view}} -n 2 -o {{output.compartments}}"


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
    params:
        extra="--bigwig",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v1.21.2/bio/cooltools/eigs_cis"


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
    params:
        track_name_col="GC",
        extra="",
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v1.21.2/bio/cooltools/eigs_trans"


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
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v1.21.2/bio/cooltools/genome/gc"


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
