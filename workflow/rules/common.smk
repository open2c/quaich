import toolz
from cooltools.lib.io import read_viewframe_from_file


def get_files(folder, extension):
    files = list(map(path.basename, glob(f"{folder}/*{extension}")))
    return files


def make_local_path(bedname, kind):
    if kind == "bed":
        return f"{beds_folder}/{bedname}.bed"
    elif kind == "bedpe":
        return f"{bedpes_folder}/{bedname}.bedpe"
    else:
        raise ValueError("Only bed and bedpe file types are supported")


def merge_dicts(dict1, dict2):
    return toolz.dicttoolz.merge_with(
        lambda x: list(toolz.itertoolz.concat(x)), dict1, dict2
    )


def split_dist(dist_wildcard, mindist_arg="--mindist", maxdist_arg="--maxdist"):
    if dist_wildcard == "":
        return ""
    else:
        assert dist_wildcard.startswith("_dist_")
        dists = dist_wildcard.split("_")[-1]
        mindist, maxdist = dists.split("-")
        return f"{mindist_arg} {mindist} {maxdist_arg} {maxdist}"


def get_shifts(norm_string):
    if norm_string.endswith("shifts"):
        shifts = int(norm_string.split("-")[0])
    else:
        shifts = 0
    return f"--nshifts {shifts}"


def get_mode_arg(mode):
    if mode == "local":
        return "--local"
    elif mode == "trans":
        return "--trans"
    else:
        return ""


def verify_view_cooler(clr):
    try:
        view = read_viewframe_from_file(
            config["view"], verify_cooler=clr, check_sorting=True
        )
        return
    except Exception as e:
        raise ValueError(f"View not compatible with cooler!") from e


def download_file(file, local_filename):
    import requests
    import tqdm
    import re

    with requests.get(file, stream=True) as r:
        ext_gz = (
            ".gz"
            if re.findall("filename=(.+)", r.headers["content-disposition"])[
                0
            ].endswith(".gz")
            else ""
        )
        r.raise_for_status()
        print("downloading:", file, "as ", local_filename + ext_gz)
        with open(local_filename + ext_gz, "wb") as f:
            for chunk in tqdm.tqdm(r.iter_content(chunk_size=8192)):
                f.write(chunk)
    return local_filename + ext_gz


def get_file(file, output):
    """
    If input is URL, it download via python's requests. Uncompress if needed.
    """
    if file == output:
        exit()

    from urllib.parse import urlparse

    parsed_path = urlparse(file)
    if parsed_path.scheme == "http" or parsed_path.scheme == "https":
        output_file = download_file(file, output)
    else:
        raise Exception(
            f"Unable to download from: {file}\nScheme {parsed_url.scheme} is not supported"
        )
    if output_file.endswith(".gz"):
        shell(f"gzip -d {output_file}")


rule download_file:
    output:
        f"{{folder,({coolers_folder}|{beds_folder})}}/{{filename}}.{{ext,(bed|bedpe|mcool)}}",
    threads: 1
    resources:
        mem_mb=256,
        runtime=60,
    params:
        file=lambda wildcards, output: coollinks_dict[output[0]]
        if wildcards.ext == "mcool"
        else bedlinks_dict[output[0]],
    run:
        if not path.exists(params.file):
            get_file(str(params.file), output[0])
        if wildcards.ext == "mcool":
            verify_view_cooler(
                cooler.Cooler(
                    f"{output[0]}::{cooler.fileops.list_coolers(output[0])[0]}"
                )
            )
