rule make_saddles:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        track=(
            f"{eigenvectors_folder}/{{sample}}_{{resolution}}_eigenvectors.cis.vecs.tsv"
        ),
        expected=f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv",
        view=lambda wildcards: config["view"],
    output:
        saddle=f"{saddles_folder}/{{sample}}_{{resolution,[0-9]+}}_{{bins,[0-9]+}}{{dist,.*}}.saddledump.npz",
        digitized_track=f"{saddles_folder}/{{sample}}_{{resolution,[0-9]+}}_{{bins,[0-9]+}}{{dist,.*}}.digitized.tsv",
    params:
        extra=lambda wildcards: " ".join(
            [
                config["saddle"]["extra"],
                split_dist(wildcards.dist, "--min-dist", "--max-dist"),
                f"--n-bins {wildcards.bins}",
            ]
        ),
        range=lambda wildcards: config["saddle"]["range"],
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        #TODO: change link back to normal when https://github.com/snakemake/snakemake-wrappers/pull/974 is merged
        "https://github.com/Phlya/snakemake-wrappers/raw/patch-2/bio/cooltools/saddle"
