import pandas as pd


rule make_differential_insulation:
    input:
        insulation_WT=(
            f"{insulation_folder}/{{sampleWT}}_{{resolution}}.insulation.tsv"
        ),
        insulation_KO=(
            f"{insulation_folder}/{{sampleKO}}_{{resolution}}.insulation.tsv"
        ),
    output:
        f"{boundaries_folder}/Diff_boundaries_{{sampleKO}}_vs_{{sampleWT}}_{{resolution,[0-9]+}}_{{window,[0-9]+}}.bed",
    threads: 1
    resources:
        mem_mb=1024,
        runtime=60,
    run:
        insWT = pd.read_csv(input.insulation_WT, sep="\t")
        insWT = insWT[~insWT["is_bad_bin"]].drop(columns=["is_bad_bin"])
        insKO = pd.read_csv(input.insulation_KO, sep="\t")
        insKO = insKO[~insKO["is_bad_bin"]].drop(columns=["is_bad_bin"])
        ins = pd.merge(
            insWT, insKO, suffixes=("WT", "KO"), on=["chrom", "start", "end"]
        )
        diff_ins = ins[
            (  # Boundary much stronger in WT vs KO
                ins[f"boundary_strength_{wildcards.window}WT"]
                / ins[f"boundary_strength_{wildcards.window}KO"]
                >= config["compare_boundaries"]["fold_change_threshold"]
            )
            | (  # OR there is a strong boundary in WT and not in KO
                ins[f"is_boundary_{wildcards.window}WT"]
                & ~ins[f"is_boundary_{wildcards.window}KO"]
            )
        ]
        diff_ins[["chrom", "start", "end"]].to_csv(
            output[0], header=False, index=False, sep="\t"
        )


rule make_tads:
    input:
        insulation=(f"{insulation_folder}/{{sample}}_{{resolution}}.insulation.tsv"),
    output:
        f"{tads_folder}/TADs_{{sample}}_{{resolution,[0-9]+}}_{{window,[0-9]+}}.bed",
    threads: 1
    resources:
        mem_mb=1024,
        runtime=60,
    run:
        ins = pd.read_csv(input.insulation, sep="\t")
        tads = bioframe.merge(ins[ins[f"is_boundary_{wildcards.window}"] == False])

        tads = tads[
            (tads["end"] - tads["start"]) <= config["TADs"]["max_tad_length"]
        ].reset_index(drop=True)
        tads.to_csv(output[0], header=False, index=False, sep="\t")


rule save_strong_boundaries:
    input:
        insulation=(f"{insulation_folder}/{{sample}}_{{resolution}}.insulation.tsv"),
    output:
        f"{boundaries_folder}/Boundaries_{{sample}}_{{resolution,[0-9]+}}_{{window,[0-9]+}}.bed",
    threads: 1
    resources:
        mem_mb=1024,
        runtime=60,
    run:
        ins = pd.read_csv(input.insulation, sep="\t")
        boundaries = ins.loc[
            ins[f"is_boundary_{wildcards.window}"], ["chrom", "start", "end"]
        ]
        boundaries.to_csv(output[0], header=False, index=False, sep="\t")


rule make_insulation:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        view=lambda wildcards: config["view"],
    output:
        f"{insulation_folder}/{{sample}}_{{resolution,[0-9]+}}.insulation.tsv",
    params:
        window=lambda wildcards: config["insulation"]["resolutions"][
            int(wildcards.resolution)
        ],
        extra=lambda wildcards: config["insulation"].get("extra", ""),
    threads: 4
    resources:
        mem_mb=32 * 1024,
        runtime=240,
    wrapper:
        "v2.6.0/bio/cooltools/insulation"
