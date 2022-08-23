# Snakemake workflow: quaich

[![Snakemake](https://img.shields.io/badge/snakemake-≥5.7.0-brightgreen.svg)](https://snakemake.bitbucket.io)
[![Build Status](https://travis-ci.org/snakemake-workflows/quaich.svg?branch=master)](https://travis-ci.org/snakemake-workflows/quaich)
[![DOI](https://zenodo.org/badge/272558705.svg)](https://zenodo.org/badge/latestdoi/272558705)



`Quaich` is a [`snakemake`](https://snakemake.readthedocs.io/en/stable/) based workflow for reproducible and flexible analysis of Hi-C data. Quaich uses multi-resolution [`cooler`](https://github.com/open2c/cooler) (.mcool) files as its input. These files can be generated efficiently by the [`distiller`](https://github.com/open2c/distiller-nf) data processing pipeline. `Quaich` takes advantage of the `open2c` ecosystem for analysis of C data, primarily making use of  command line tools from [`cooltools`](https://github.com/open2c/cooltools). `Quaich` also makes use of [chromosight](https://github.com/koszullab/chromosight) and [mustache](https://github.com/ay-lab/mustache) to call Hi-C peaks (peaks, dots) as well as [coolpuppy](https://github.com/open2c/coolpuppy) to generate lots of pileups.

[Snakemake](https://snakemake.readthedocs.io/en/stable/) is a workflow manager for reproducible and scalable data analyses, based around the concept of rules. Rules used in `Quaich` are defined in the [Snakefile](https://github.com/open2c/quaich/blob/master/workflow/Snakefile). `Quaich` then uses a [yaml config file](https://github.com/open2c/quaich/blob/master/config/config.yaml) to specify which rules to run, and which parameters to use for those rules.

Each rule in the `Quaich` Snakefile specifies inputs, outputs, resources, and threads. The best values for resources and threads depend on whether `quaich` is run locally or on a cluster. Outputs of one rule are often used as inputs to another. For example, the rule `make_expected` calls `cooltools compute-expected` on a mcool for a set of regions at specified resolutions to output tsv. This output is then used in make_saddles, make_pileups, and call_loops_cooltools.
```python
rule make_expected:
    input:
        cooler=lambda wildcards: coolfiles_dict[wildcards.sample],
        regions=lambda wildcards: config["regions"],
    output:
        f"{expected_folder}/{{sample}}_{{resolution,[0-9]+}}.expected.tsv",
    threads: 4 
    resources:
        mem_mb=lambda wildcards, threads: threads * 8 * 1024,
        runtime=60,
    shell:
        "cooltools compute-expected -p {threads} {input.cooler}::resolutions/{wildcards.resolution} --regions {input.regions} --ignore-diags 0 -o {output}"
```

`Quaich` groups similar rules together in config.yaml, which is read as a python dictionary by the Snakefile. Parameters for individual rules are passed as indented (key, value) pairs. For example, call_dots configures three rules, call_loops_cooltools,  call_loops_chromosight, and call_loops_mustache. The parameters for each specific rule is underneath, the shared parameters are below (e.g. resolutions, and samples).  call_loops_cooltools has parameters do, max_dist, fdr. `Do` always specifies if the workflow should attempt to produce the output for this rule.
``` call_dots:
    methods:
        cooltools:
            do: True
            max_dist: 10000000
            fdr: 0.02
            arguments: ""
        chromosight:
            do: True
        mustache:
            do: True
            max_dist: 10000000
            arguments: "-pt 0.05 -st 0.8"
    resolutions:
        - 10000
    samples:
        - test_cool
```

`quaich`  config.yaml has four main sections:
- genome
- annotations
- i/o 
- snakemake rule configurations


The following analyses can be configured:
- eigenvector: calculates cis eigenvectors using cooltools for all resolutions within specified _resolution_limits_. 
- saddle: calculates saddles, reflecting average interaction preferences, from cis eigenvectors for each sample using cooltools.
- pileups: does everything :)
- loopability: tba
- insulation: calculates diamond insulation score for specified resolutions and window sizes, using cooltools. Currently runs separately for different window sizes. 
- call_dots: three methods of calling dots, at specified resolutions, and postprocess output to bedpe. Implemented callers are cooltools, mustache and chromosight. Only runs on specified samples. 
 - compare_boundaries: generates differential boundaries between specified samples, used as input for pileups.
- call_TADs: combines lists of strong boundaries for specified samples into a list across window sizes for each resolution, filtered by length, used as input for pileups. 

## Authors

* Ilya Flyamer (@phlya)

## Usage

If you use this workflow in a paper, don't forget to give credits to the authors by citing the URL of this (original) repository.

### Step 1: Obtain a copy of this workflow

1. Create a new github repository using this workflow [as a template](https://help.github.com/en/articles/creating-a-repository-from-a-template).
2. [Clone](https://help.github.com/en/articles/cloning-a-repository) the newly created repository to your local system, into the place where you want to perform the data analysis.

### Step 2: Configure workflow

Configure the workflow according to your needs via editing the files in the `config/` folder. Adjust `config.yaml` to configure the workflow execution, and `samples.tsv` to specify your sample setup. If you want to use any external bed or bedpe files for pileups, describe them in the `annotations.tsv` file, and pairings of samples with annotations in `samples_annotations.tsv`.

### Step 3: Install Snakemake and other requirements

Install Snakemake and other requirements using [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html):

    conda env create -f workflow/envs/environment.yml

This will create an environment `quaich` where you can launch the pipeline.

For Snakemake installation details, see the [instructions in the Snakemake documentation](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html).

### Step 4: Execute workflow

Activate the conda environment:

    conda activate quaich

Test your configuration by performing a dry-run via

    snakemake --use-conda --configfile config/config.yml -n

Execute the workflow locally via

    snakemake --use-conda --configfile config/config.yml --cores $N

using `$N` cores or run it in a cluster environment via

    snakemake --use-conda --configfile config/config.yml --cluster qsub --jobs 100

or

    snakemake --use-conda --configfile config/config.yml --drmaa --jobs 100

Alternatively, you might want to look into snakemake profiles already available for your HPC scheduler online, for example, [here](https://github.com/Snakemake-Profiles) or elsewhere.

If you not only want to fix the software stack but also the underlying OS, use

    snakemake --use-conda --configfile config/config.yml --use-singularity

in combination with any of the modes above. *(Not yet available)*
See the [Snakemake documentation](https://snakemake.readthedocs.io/en/stable/executable.html) for further details.


### Step 5: Investigate results *not available yet*

After successful execution, you can create a self-contained interactive HTML report with all results via:

    snakemake --report report.html

This report can, e.g., be forwarded to your collaborators.
An example (using some trivial test data) can be seen [here](https://cdn.rawgit.com/snakemake-workflows/rna-seq-kallisto-sleuth/master/.test/report.html).

### Step 6: Commit changes

Whenever you change something, don't forget to commit the changes back to your github copy of the repository:

    git commit -a
    git push

### Step 7: Obtain updates from upstream

Whenever you want to synchronize your workflow copy with new developments from upstream, do the following.

1. Once, register the upstream repository in your local copy: `git remote add -f upstream git@github.com:snakemake-workflows/quaich.git` or `git remote add -f upstream https://github.com/snakemake-workflows/quaich.git` if you do not have setup ssh keys.
2. Update the upstream version: `git fetch upstream`.
3. Create a diff with the current version: `git diff HEAD upstream/master workflow > upstream-changes.diff`.
4. Investigate the changes: `vim upstream-changes.diff`.
5. Apply the modified diff via: `git apply upstream-changes.diff`.
6. Carefully check whether you need to update the config files: `git diff HEAD upstream/master config`. If so, do it manually, and only where necessary, since you would otherwise likely overwrite your settings and samples.


### Step 8: Contribute back

In case you have also changed or added steps, please consider contributing them back to the original repository:

1. [Fork](https://help.github.com/en/articles/fork-a-repo) the original repo to a personal or lab account.
2. [Clone](https://help.github.com/en/articles/cloning-a-repository) the fork to your local system, to a different place than where you ran your analysis.
3. Copy the modified files from your analysis to the clone of your fork, e.g., `cp -r workflow path/to/fork`. Make sure to **not** accidentally copy config file contents or sample sheets. Instead, manually update the example config files if necessary.
4. Commit and push your changes to your fork.
5. Create a [pull request](https://help.github.com/en/articles/creating-a-pull-request) against the original repository.

## Testing

Test cases are in the subfolder `.test`. They are automatically executed via continuous integration with [Github Actions](https://github.com/features/actions).
