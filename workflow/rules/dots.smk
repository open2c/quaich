def dedup_dots(dots, hiccups_filter=False):
    newdots = []
    ress = list(sorted(set(dots["res"])))
    for chrom in sorted(set(dots["chrom1"])):
        chromdots = (
            dots[dots["chrom1"] == chrom]
            .sort_values(["start1", "start2"])
            .reset_index(drop=True)
        )
        for res in ress:
            chromdots["Supported_%s" % res] = chromdots["res"] == res
        # TODO fix!
        tree = spatial.cKDTree(chromdots[["start1", "start2"]])
        drop = []
        for i, j in tree.query_pairs(r=20000):
            ires = chromdots.at[i, "res"]
            jres = chromdots.at[j, "res"]
            chromdots.at[j, "Supported_%s" % ires] = True
            chromdots.at[i, "Supported_%s" % jres] = True
            if ires == jres:
                continue
            elif ires > jres:
                # if ress[-1] in (ires, jres) or abs(chromdots.at[j, 'start1']-chromdots.at[i, 'start1'])<=20000:
                drop.append(i)
            else:
                drop.append(j)
        newdots.append(chromdots.drop(drop))
    deduped = pd.concat(newdots).sort_values(["chrom1", "start1", "start2"])
    if hiccups_filter:
        l = len(deduped)
        deduped = deduped[
            ~(
                (deduped["start2"] - deduped["start1"] > 100000)
                & (~np.any(deduped[["Supported_%s" % res for res in ress[1:]]], axis=1))
            )
        ]
        print(
            l - len(deduped),
            "dots filtered out as unreliable %s resolution calls" % ress[0],
        )
    return deduped


def read_dots(f):
    df = pd.read_table(f, index_col=False, header=0).dropna(axis=1)
    res = int(f.split("_")[-1].split(".")[0])
    df["res"] = res
    return df


rule merge_dots_across_resolutions:
    input:
        dots=lambda wildcards,: [
            f"{dots_folder}/Dots_{{method}}_{{sample}}_{resolution}.bedpe"
            for resolution in config["dots"]["resolutions"]
        ],
    output:
        f"{dots_folder}/Dots_{{method}}_{{sample}}.bedpe",
    log:
        "logs/merge_dots_across_resolutions/{method}_{sample}.log",
    threads: 1
    resources:
        mem_mb=lambda wildcards, threads: 1024,
        runtime=5,
    run:
        dots = pd.concat((map(read_dots, input.dots))).reset_index(drop=True)
        dots = dedup_dots(dots)[
            ["chrom1", "start1", "end1", "chrom2", "start2", "end2"]
        ]
        dots.to_csv(output[0], sep="\t", header=False, index=False)


rule call_dots_cooltools:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        expected=f"{expected_folder}/{{sample}}_{{resolution}}.expected.tsv",
        view=lambda wildcards: config["view"],
    output:
        f"{dots_folder}/Dots_cooltools_{{sample}}_{{resolution,[0-9]+}}.bedpe",
    log:
        "logs/call_dots_cooltools/{sample}_{resolution}.log",
    threads: 4
    params:
        extra=lambda wildcards: config["dots"]["methods"]["cooltools"]["extra"],
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    wrapper:
        "v2.6.0/bio/cooltools/dots"


rule call_dots_chromosight:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
    output:
        bedpe=f"{dots_folder}/Dots_chromosight_{{sample}}_{{resolution,[0-9]+}}.bedpe",
        json=f"{dots_folder}/Dots_chromosight_{{sample}}_{{resolution,[0-9]+}}.json",
    log:
        "logs/call_dots_chromosight/{sample}_{resolution}.log",
    threads: 4
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    conda:
        "../envs/chromosight_env.yml"
    shell:
        f"chromosight detect --pattern loops --no-plotting -t {{threads}} {{input.cooler}}::resolutions/{{wildcards.resolution}} {dots_folder}/Dots_chromosight_{{wildcards.sample}}_{{wildcards.resolution}} && "
        f"mv {dots_folder}/Dots_chromosight_{{wildcards.sample}}_{{wildcards.resolution}}.tsv {{output.bedpe}}"


rule call_dots_mustache:
    input:
        f"{dots_folder}/Dots_mustache_{{sample}}_{{resolution}}.bedpe_tmp",
    output:
        f"{dots_folder}/Dots_mustache_{{sample}}_{{resolution,[0-9]+}}.bedpe",
    log:
        "logs/call_dots_mustache/{sample}_{resolution}.log",
    shell:
        """TAB=$(printf '\t') && cat {input} | sed "1s/.*/chrom1${{TAB}}start1${{TAB}}end1${{TAB}}chrom2${{TAB}}start2${{TAB}}end2${{TAB}}FDR${{TAB}}detection_scale/" > {output}"""


rule _call_dots_mustache:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
    output:
        temp(f"{dots_folder}/Dots_mustache_{{sample}}_{{resolution,[0-9]+}}.bedpe_tmp"),
    log:
        "logs/_call_dots_mustache/{sample}_{resolution}.log",
    threads: 4
    params:
        args=config["dots"]["methods"]["mustache"]["extra"],
        dist=config["dots"]["methods"]["mustache"]["max_dist"],
    resources:
        mem_mb=lambda wildcards, threads: threads * 16 * 1024,
        runtime=24 * 60,
    conda:
        "../envs/mustache_env.yml"
    shell:
        f"python3 -m mustache -p {{threads}} -f {{input.cooler}} -r {{wildcards.resolution}} "
        f"-d {{params.dist}} {{params.args}} -o {{output}}"
