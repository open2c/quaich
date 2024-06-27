import argparse
from coolpuppy.lib import io as cpio

parser = argparse.ArgumentParser()
parser.add_argument("-i1", "--input1", required=True, type=str)
parser.add_argument("-i2", "--input2", required=True, type=str)
parser.add_argument("--groupby", required=True, nargs="*")
parser.add_argument("-o", "--output", required=True)

args = parser.parse_args()

pentads1 = cpio.load_pileup_df(args.input1)
pentads2 = cpio.load_pileup_df(args.input2)
merged = pentads1.merge(
    pentads2,
    on=["name1", "name2", "local"] + args.groupby,
    suffixes=["_1", "_2"],
)
merged["data"] = merged["data_1"] / merged["data_2"]
merged["store_stripes"] = merged["store_stripes_1"]
merged = merged.drop(columns=["store_stripes_1", "store_stripes_2"])
cpio.save_pileup_df(args.output, merged)
