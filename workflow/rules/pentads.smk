from os import path


rule make_diff_pentads:
    input:
        pentads1=lambda wildcards: f"{pentads_folder}/{{sample1}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_pentads.clpy",
        pentads2=lambda wildcards: f"{pentads_folder}/{{sample2}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_pentads.clpy",
    output:
        pantads_ratio=f"{pentads_folder}/diff/{{sample1}}_vs_{{sample2}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_pentads.clpy",
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
    threads: 1
    resources:
        mem_mb=lambda wildcards: 512,
        runtime=24 * 60,
    run:
        from coolpuppy.lib import io as cpio

        pentads1 = cpio.load_pileup_df(input.pentads1)
        pentads2 = cpio.load_pileup_df(input.pentads2)
        merged = pentads1.merge(
            pentads2,
            on=["name1", "name2", "local"] + config["pentads"]["groupby"],
            suffixes=["_1", "_2"],
        )
        merged["data"] = merged["data_1"] / merged["data_2"]
        merged["store_stripes"] = merged["store_stripes_1"]
        merged = merged.drop(columns=["store_stripes_1", "store_stripes_2"])
        cpio.save_pileup_df(output.pantads_ratio, merged)


# Merges different pentad squares into one file
rule make_pentads:
    input:
        pentads_local=lambda wildcards: f"{pentads_folder}/{{sample}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_local_pentads.clpy",
        pentads_distal=lambda wildcards: f"{pentads_folder}/{{sample}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_distal_pentads.clpy",
    output:
        pentads=f"{pentads_folder}/{{sample}}_{config['pentads']['data_resolution']}_over_compartments_{{sample_ref}}_{config['pentads']['eigenvector_resolution']}_{{norm}}_pentads.clpy",
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
    threads: 1
    resources:
        mem_mb=lambda wildcards: 512,
        runtime=24 * 60,
    run:
        from coolpuppy.lib import io as cpio

        pentads = cpio.load_pileup_df_list([input.pentads_local, input.pentads_distal])
        cpio.save_pileup_df(output.pentads, pentads)


rule _make_pentad_components:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        features=lambda wildcards: f"{compartments_folder}/{{sample_ref}}_{{eigenvector_resolution}}_compartments.cis.bed",
        expected=lambda wildcards: f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv"
        if wildcards.norm == "expected" and wildcards.mode != "trans"
        else f"{expected_folder}/{{sample}}_{{resolution}}.expected.trans.tsv"
        if wildcards.norm == "expected" and wildcards.mode == "trans"
        else [],
        view=lambda wildcards: config["view"],  # if wildcards.norm == "expected" else [],
    output:
        temp(
            f"{pentads_folder}/{{sample}}_{{resolution}}_over_compartments_{{sample_ref}}_{{eigenvector_resolution}}_{{norm}}_{{mode}}_pentads.clpy"
        ),
    wildcard_constraints:
        norm="(expected|nonorm|[0-9]+\-shifts)",
        mode="(local|distal|trans)",
    params:
        features_format="bed",
        extra=lambda wildcards: f"--rescale --rescale_flank 0 --rescale_size 33 "
        f"--groupby name1 name2 {' '.join(config['pentads']['groupby'])} "
        f"--ignore-diags 0 {get_mode_arg(wildcards.mode)} "
        f"--nshifts {wildcards.norm.split('-')[0] if wildcards.norm.endswith('shifts') else '0'}",
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    wrapper:
        "v2.0.0/bio/coolpuppy"
