rule make_saddles:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        track=(
            f"{eig_profiles_folder}/{{sampleTrack}}_{{resolution}}_eigenvectors.cis.vecs.tsv"
        ),
        expected=f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv",
        view=lambda wildcards: config["view"],
    output:
        saddle=f"{saddles_folder}/{{sample}}_{{resolution,[0-9]+}}_over_{{sampleTrack}}_eig_{{bins,[0-9]+}}{{dist,.*}}.saddledump.npz",
        digitized_track=f"{saddles_folder}/{{sample}}_{{resolution,[0-9]+}}_over_{{sampleTrack}}_eig_{{bins,[0-9]+}}{{dist,.*}}.digitized.tsv",
    benchmark:
        "benchmarks/make_saddles/{sample}_{resolution,[0-9]+}_over_{sampleTrack}_eig_{bins,[0-9]+}{dist,.*}.tsv"
    log:
        "logs/make_saddles/{sample}}_{resolution,[0-9]+}_over_{sampleTrack}_eig_{bins,[0-9]+}{{dist,.*}.tsv",
    params:
        extra=lambda wildcards: " ".join(
            [
                config["saddles"]["extra"],
                split_dist(wildcards.dist, "--min-dist", "--max-dist"),
                f"--n-bins {wildcards.bins}",
            ]
        ),
        range=lambda wildcards: config["saddles"]["range"],
    threads: 1
    resources:
        mem_mb=8 * 1024,
        runtime=60,
    wrapper:
        "v3.12.2/bio/cooltools/saddle"
